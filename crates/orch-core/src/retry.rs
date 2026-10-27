//! Retry policy. Outcomes are classified first: infrastructure failures (crash,
//! timeout, lost lease) are always retryable; a dbt node failure (a failing test, a
//! model SQL error) is retried only if the job opts in, because re-running the
//! same SQL against the same data usually fails the same way.

use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    pub retry_on_node_failure: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay_ms: 2_000,
            max_delay_ms: 60_000,
            retry_on_node_failure: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptOutcome {
    Succeeded,
    /// dbt exited 1 and wrote run_results: some node errored or a test failed.
    NodeFailure,
    /// dbt crashed, exited 2, could not start, or produced no run_results.
    InfraError,
    TimedOut,
    /// The worker stopped heartbeating; the lease expired and was reclaimed.
    Lost,
    Cancelled,
}

impl AttemptOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::NodeFailure => "node_failure",
            Self::InfraError => "infra_error",
            Self::TimedOut => "timed_out",
            Self::Lost => "lost",
            Self::Cancelled => "cancelled",
        }
    }

    fn retryable(self, policy: &RetryPolicy) -> bool {
        match self {
            Self::InfraError | Self::TimedOut | Self::Lost => true,
            Self::NodeFailure => policy.retry_on_node_failure,
            Self::Succeeded | Self::Cancelled => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    Done,
    RetryAfter(Duration),
    GiveUp,
}

/// `attempt_no` is 1-based: the attempt that just ended.
pub fn decide(
    policy: &RetryPolicy,
    attempt_no: u32,
    outcome: AttemptOutcome,
    jitter_seed: u64,
) -> RetryDecision {
    if outcome == AttemptOutcome::Succeeded {
        return RetryDecision::Done;
    }
    if !outcome.retryable(policy) || attempt_no >= policy.max_attempts {
        return RetryDecision::GiveUp;
    }
    RetryDecision::RetryAfter(backoff(policy, attempt_no, jitter_seed))
}

/// Exponential backoff `base * 2^(n-1)`, capped at `max`, with "equal jitter":
/// half the delay is fixed, half is spread by the seed. The seed is derived from
/// the step id, so retries of different steps spread out but a given step's
/// schedule is reproducible (and testable).
pub fn backoff(policy: &RetryPolicy, attempt_no: u32, jitter_seed: u64) -> Duration {
    let exp = attempt_no.saturating_sub(1).min(32);
    let raw = policy.base_delay_ms.saturating_mul(1u64 << exp);
    let capped = raw.min(policy.max_delay_ms);
    let half = capped / 2;
    Duration::from_millis(half + jitter_seed % (capped - half + 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 4,
            base_delay_ms: 1_000,
            max_delay_ms: 5_000,
            retry_on_node_failure: false,
        }
    }

    #[test]
    fn backoff_doubles_and_caps_within_jitter_bounds() {
        let p = policy();
        for seed in [0u64, 1, 7, 999, u64::MAX] {
            for (n, full) in [
                (1u32, 1_000u64),
                (2, 2_000),
                (3, 4_000),
                (4, 5_000),
                (30, 5_000),
            ] {
                let d = backoff(&p, n, seed).as_millis() as u64;
                assert!(d >= full / 2 && d <= full, "n={n} seed={seed} d={d}");
            }
        }
        // deterministic for a given seed
        assert_eq!(backoff(&p, 2, 42), backoff(&p, 2, 42));
    }

    #[test]
    fn infra_failures_retry_until_max_attempts() {
        let p = policy();
        for o in [
            AttemptOutcome::InfraError,
            AttemptOutcome::TimedOut,
            AttemptOutcome::Lost,
        ] {
            assert!(matches!(decide(&p, 1, o, 0), RetryDecision::RetryAfter(_)));
            assert!(matches!(decide(&p, 3, o, 0), RetryDecision::RetryAfter(_)));
            assert_eq!(decide(&p, 4, o, 0), RetryDecision::GiveUp);
        }
    }

    #[test]
    fn node_failures_retry_only_when_opted_in() {
        let mut p = policy();
        assert_eq!(
            decide(&p, 1, AttemptOutcome::NodeFailure, 0),
            RetryDecision::GiveUp
        );
        p.retry_on_node_failure = true;
        assert!(matches!(
            decide(&p, 1, AttemptOutcome::NodeFailure, 0),
            RetryDecision::RetryAfter(_)
        ));
    }

    #[test]
    fn success_and_cancel_never_retry() {
        let p = policy();
        assert_eq!(
            decide(&p, 1, AttemptOutcome::Succeeded, 0),
            RetryDecision::Done
        );
        assert_eq!(
            decide(&p, 1, AttemptOutcome::Cancelled, 0),
            RetryDecision::GiveUp
        );
    }

    #[test]
    fn huge_attempt_numbers_do_not_overflow() {
        let p = RetryPolicy {
            max_attempts: u32::MAX,
            base_delay_ms: u64::MAX / 2,
            max_delay_ms: u64::MAX,
            retry_on_node_failure: true,
        };
        let _ = backoff(&p, u32::MAX, u64::MAX);
    }
}
