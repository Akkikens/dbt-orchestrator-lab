//! Run planning: given the current status of every step in a run, decide which
//! pending steps become runnable, which are skipped because an upstream step
//! failed, which are cancelled, and whether the run is finished. The store calls
//! this inside the transaction that records a step's result, so the decision and
//! the state it was made from cannot drift apart.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    Pending,
    Queued,
    Running,
    Succeeded,
    Failed,
    Skipped,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

macro_rules! str_enum {
    ($t:ty { $($v:ident => $s:literal),* $(,)? }) => {
        impl $t {
            pub fn as_str(self) -> &'static str {
                match self { $(Self::$v => $s),* }
            }
            pub fn parse(s: &str) -> Option<Self> {
                match s { $($s => Some(Self::$v),)* _ => None }
            }
        }
    };
}

str_enum!(StepStatus {
    Pending => "pending", Queued => "queued", Running => "running",
    Succeeded => "succeeded", Failed => "failed", Skipped => "skipped", Cancelled => "cancelled",
});
str_enum!(RunStatus {
    Queued => "queued", Running => "running", Succeeded => "succeeded",
    Failed => "failed", Cancelled => "cancelled",
});

impl StepStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Skipped | Self::Cancelled
        )
    }
    fn blocks_dependents(self) -> bool {
        matches!(self, Self::Failed | Self::Skipped | Self::Cancelled)
    }
}

impl RunStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone)]
pub struct StepView {
    pub name: String,
    pub depends_on: Vec<String>,
    pub status: StepStatus,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Plan {
    pub queue: Vec<String>,
    pub skip: Vec<String>,
    pub cancel: Vec<String>,
    pub outcome: Option<RunStatus>,
}

/// Running steps are never touched here: cancelling one is the worker's job (it
/// sees the run's cancel flag on its next heartbeat and kills dbt). The run only
/// becomes terminal once nothing is running.
pub fn plan(steps: &[StepView], cancel_requested: bool) -> Plan {
    let mut eff: HashMap<&str, StepStatus> =
        steps.iter().map(|s| (s.name.as_str(), s.status)).collect();
    let mut out = Plan::default();

    if cancel_requested {
        for s in steps {
            if matches!(s.status, StepStatus::Pending | StepStatus::Queued) {
                eff.insert(&s.name, StepStatus::Cancelled);
                out.cancel.push(s.name.clone());
            }
        }
    } else {
        // Skips cascade (a -> b -> c: a failing skips b, which skips c), so iterate
        // to a fixpoint. Each pass changes at least one step or stops, so this is
        // bounded by the number of steps.
        loop {
            let mut changed = false;
            for s in steps {
                if eff[s.name.as_str()] != StepStatus::Pending {
                    continue;
                }
                let deps: Vec<StepStatus> = s.depends_on.iter().map(|d| eff[d.as_str()]).collect();
                if deps.iter().any(|d| d.blocks_dependents()) {
                    eff.insert(&s.name, StepStatus::Skipped);
                    out.skip.push(s.name.clone());
                    changed = true;
                } else if deps.iter().all(|d| *d == StepStatus::Succeeded) {
                    eff.insert(&s.name, StepStatus::Queued);
                    out.queue.push(s.name.clone());
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
    }

    if eff.values().all(|s| s.is_terminal()) {
        out.outcome = Some(if cancel_requested {
            RunStatus::Cancelled
        } else if eff.values().any(|s| *s == StepStatus::Failed) {
            RunStatus::Failed
        } else if eff.values().any(|s| *s == StepStatus::Cancelled) {
            RunStatus::Cancelled
        } else {
            RunStatus::Succeeded
        });
    }
    out
}

/// How many queued runs of a job may start now under its concurrency limit.
pub fn admit_count(max_concurrency: u32, running: u32, queued: u32) -> u32 {
    queued.min(max_concurrency.saturating_sub(running))
}

#[cfg(test)]
mod tests {
    use super::*;
    use StepStatus::*;

    fn s(name: &str, deps: &[&str], status: StepStatus) -> StepView {
        StepView {
            name: name.into(),
            depends_on: deps.iter().map(|d| d.to_string()).collect(),
            status,
        }
    }

    #[test]
    fn roots_are_queued_first() {
        let p = plan(
            &[s("seed", &[], Pending), s("build", &["seed"], Pending)],
            false,
        );
        assert_eq!(p.queue, vec!["seed"]);
        assert!(p.outcome.is_none());
    }

    #[test]
    fn dependents_queue_once_all_parents_succeed() {
        let steps = [
            s("a", &[], Succeeded),
            s("b", &[], Running),
            s("c", &["a", "b"], Pending),
        ];
        assert!(plan(&steps, false).queue.is_empty());
        let steps = [
            s("a", &[], Succeeded),
            s("b", &[], Succeeded),
            s("c", &["a", "b"], Pending),
        ];
        assert_eq!(plan(&steps, false).queue, vec!["c"]);
    }

    #[test]
    fn failure_cascades_skips_and_fails_the_run() {
        let steps = [
            s("a", &[], Failed),
            s("b", &["a"], Pending),
            s("c", &["b"], Pending),
            s("d", &[], Succeeded),
        ];
        let p = plan(&steps, false);
        assert_eq!(p.skip, vec!["b", "c"]);
        assert_eq!(p.outcome, Some(RunStatus::Failed));
    }

    #[test]
    fn failed_run_waits_for_running_siblings() {
        let steps = [s("a", &[], Failed), s("b", &[], Running)];
        assert_eq!(plan(&steps, false).outcome, None);
    }

    #[test]
    fn all_succeeded_finishes_the_run() {
        let steps = [s("a", &[], Succeeded), s("b", &["a"], Succeeded)];
        assert_eq!(plan(&steps, false).outcome, Some(RunStatus::Succeeded));
    }

    #[test]
    fn cancel_stops_pending_and_queued_but_waits_for_running() {
        let steps = [
            s("a", &[], Running),
            s("b", &[], Queued),
            s("c", &["a"], Pending),
        ];
        let p = plan(&steps, true);
        assert_eq!(p.cancel, vec!["b", "c"]);
        assert!(p.queue.is_empty());
        assert_eq!(p.outcome, None, "a is still running");
        let steps = [
            s("a", &[], Cancelled),
            s("b", &[], Cancelled),
            s("c", &["a"], Cancelled),
        ];
        assert_eq!(plan(&steps, true).outcome, Some(RunStatus::Cancelled));
    }

    #[test]
    fn admission_respects_concurrency_limit() {
        assert_eq!(admit_count(2, 0, 5), 2);
        assert_eq!(admit_count(2, 1, 5), 1);
        assert_eq!(admit_count(2, 2, 5), 0);
        assert_eq!(admit_count(2, 3, 5), 0, "over limit after a config change");
        assert_eq!(admit_count(4, 0, 1), 1);
    }

    #[test]
    fn status_strings_round_trip() {
        for st in [
            Pending, Queued, Running, Succeeded, Failed, Skipped, Cancelled,
        ] {
            assert_eq!(StepStatus::parse(st.as_str()), Some(st));
        }
        assert_eq!(RunStatus::parse("bogus"), None);
    }
}
