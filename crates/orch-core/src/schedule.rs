//! Cron evaluation. The scheduler keeps a per-job watermark (`last_scheduled_at`)
//! and, on each tick, fires at most one run: the latest cron time in
//! `(watermark, now]`. Missed ticks (scheduler down) are coalesced into that one
//! run instead of a burst of backfill. The run's idempotency key is derived from
//! the job and the cron time, so two scheduler replicas, or a tick retried after a
//! crash, insert the same run at most once.

use std::str::FromStr;

use chrono::{DateTime, SecondsFormat, Utc};

#[derive(Debug, Clone)]
pub struct CronSchedule {
    inner: cron::Schedule,
}

impl CronSchedule {
    /// Accepts standard 5-field cron (`*/5 * * * *`) or 6/7-field with seconds.
    pub fn parse(expr: &str) -> Result<Self, String> {
        let fields = expr.split_whitespace().count();
        let normalized = match fields {
            5 => format!("0 {expr}"),
            6 | 7 => expr.to_string(),
            n => return Err(format!("expected 5-7 fields, got {n}")),
        };
        cron::Schedule::from_str(&normalized)
            .map(|inner| Self { inner })
            .map_err(|e| e.to_string())
    }

    /// Latest fire time `t` with `after < t <= now`.
    pub fn latest_fire(&self, after: DateTime<Utc>, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.inner.after(&after).take_while(|t| *t <= now).last()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TickDecision {
    pub fire_at: Option<DateTime<Utc>>,
    pub watermark: DateTime<Utc>,
}

/// What a scheduler tick should do for one cron job. A job seen for the first
/// time (no watermark) does not fire: registering a job is not a reason to run it.
pub fn decide_tick(
    schedule: &CronSchedule,
    watermark: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> TickDecision {
    match watermark {
        None => TickDecision {
            fire_at: None,
            watermark: now,
        },
        Some(w) if w >= now => TickDecision {
            fire_at: None,
            watermark: w,
        },
        Some(w) => TickDecision {
            fire_at: schedule.latest_fire(w, now),
            watermark: now,
        },
    }
}

pub fn cron_idempotency_key(job: &str, fire_at: DateTime<Utc>) -> String {
    format!(
        "cron:{job}:{}",
        fire_at.to_rfc3339_opts(SecondsFormat::Secs, true)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn t(h: u32, m: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, h, m, s).unwrap()
    }

    #[test]
    fn five_field_cron_is_minute_resolution() {
        let s = CronSchedule::parse("*/15 * * * *").unwrap();
        assert_eq!(s.latest_fire(t(10, 0, 0), t(10, 31, 0)), Some(t(10, 30, 0)));
        assert!(CronSchedule::parse("* *").is_err());
    }

    #[test]
    fn first_sight_sets_watermark_without_firing() {
        let s = CronSchedule::parse("* * * * *").unwrap();
        let d = decide_tick(&s, None, t(10, 5, 30));
        assert_eq!(d.fire_at, None);
        assert_eq!(d.watermark, t(10, 5, 30));
    }

    #[test]
    fn missed_ticks_coalesce_into_the_latest() {
        let s = CronSchedule::parse("*/5 * * * *").unwrap();
        // scheduler was down from 10:01 to 10:23: 10:05, 10:10, 10:15, 10:20 missed
        let d = decide_tick(&s, Some(t(10, 1, 0)), t(10, 23, 0));
        assert_eq!(d.fire_at, Some(t(10, 20, 0)));
        assert_eq!(d.watermark, t(10, 23, 0));
    }

    #[test]
    fn repeated_tick_in_same_window_does_not_refire() {
        let s = CronSchedule::parse("*/5 * * * *").unwrap();
        let first = decide_tick(&s, Some(t(10, 1, 0)), t(10, 6, 0));
        assert_eq!(first.fire_at, Some(t(10, 5, 0)));
        let second = decide_tick(&s, Some(first.watermark), t(10, 7, 0));
        assert_eq!(second.fire_at, None);
        // a clock that went backwards never fires or moves the watermark back
        let back = decide_tick(&s, Some(t(10, 7, 0)), t(10, 6, 0));
        assert_eq!(
            back,
            TickDecision {
                fire_at: None,
                watermark: t(10, 7, 0)
            }
        );
    }

    #[test]
    fn idempotency_key_is_stable_per_fire_time() {
        assert_eq!(
            cron_idempotency_key("nightly", t(2, 0, 0)),
            "cron:nightly:2026-01-01T02:00:00Z"
        );
    }
}
