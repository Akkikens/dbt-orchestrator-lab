//! The lease protocol for a step, as an in-memory model of the `steps` row's
//! lease columns. `orchestrator::store` implements the same rules in SQL; this
//! model exists so the rules can be tested exhaustively without a database.
//!
//! * `acquire` hands the step to a worker with a TTL and bumps the **epoch**
//!   (a fencing token). Only a free slot can be acquired.
//! * `renew` (heartbeat) needs the current epoch and an unexpired lease. A worker
//!   that misses heartbeats loses the lease even if nobody has reclaimed it yet,
//!   and must stop the dbt process.
//! * `reap` frees a slot whose lease expired (the scheduler does this) so another
//!   worker can acquire it under a new epoch.
//! * `complete` is accepted only for the current epoch while still held. A zombie
//!   worker that wakes up after being reaped presents a stale epoch and its result
//!   is discarded, so a step's result is recorded once even if dbt ran twice.

use chrono::{DateTime, Duration, Utc};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub owner: String,
    pub epoch: u64,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LeaseError {
    #[error("held by {owner} until {until}")]
    Held { owner: String, until: DateTime<Utc> },
    #[error("stale epoch {presented} (current {current})")]
    Fenced { presented: u64, current: u64 },
    #[error("lease expired")]
    Expired,
    #[error("not leased")]
    NotHeld,
}

#[derive(Debug, Default, Clone)]
pub struct LeaseSlot {
    epoch: u64,
    holder: Option<Lease>,
}

pub fn is_expired(expires_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    expires_at <= now
}

impl LeaseSlot {
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn holder(&self) -> Option<&Lease> {
        self.holder.as_ref()
    }

    pub fn acquire(
        &mut self,
        worker: &str,
        now: DateTime<Utc>,
        ttl: Duration,
    ) -> Result<Lease, LeaseError> {
        if let Some(l) = &self.holder {
            return Err(LeaseError::Held {
                owner: l.owner.clone(),
                until: l.expires_at,
            });
        }
        self.epoch += 1;
        let lease = Lease {
            owner: worker.to_string(),
            epoch: self.epoch,
            expires_at: now + ttl,
        };
        self.holder = Some(lease.clone());
        Ok(lease)
    }

    fn check(&self, epoch: u64) -> Result<&Lease, LeaseError> {
        let l = self.holder.as_ref().ok_or(LeaseError::NotHeld)?;
        if l.epoch != epoch {
            return Err(LeaseError::Fenced {
                presented: epoch,
                current: l.epoch,
            });
        }
        Ok(l)
    }

    pub fn renew(
        &mut self,
        epoch: u64,
        now: DateTime<Utc>,
        ttl: Duration,
    ) -> Result<Lease, LeaseError> {
        let l = self.check(epoch).map_err(|e| match e {
            LeaseError::NotHeld => LeaseError::Fenced {
                presented: epoch,
                current: self.epoch,
            },
            e => e,
        })?;
        if is_expired(l.expires_at, now) {
            return Err(LeaseError::Expired);
        }
        let renewed = Lease {
            expires_at: now + ttl,
            ..l.clone()
        };
        self.holder = Some(renewed.clone());
        Ok(renewed)
    }

    /// Returns true if an expired lease was freed.
    pub fn reap(&mut self, now: DateTime<Utc>) -> bool {
        match &self.holder {
            Some(l) if is_expired(l.expires_at, now) => {
                self.holder = None;
                true
            }
            _ => false,
        }
    }

    pub fn complete(&mut self, epoch: u64) -> Result<(), LeaseError> {
        self.check(epoch).map_err(|e| match e {
            LeaseError::NotHeld => LeaseError::Fenced {
                presented: epoch,
                current: self.epoch,
            },
            e => e,
        })?;
        self.holder = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(s: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_800_000_000 + s, 0).unwrap()
    }
    const TTL: Duration = Duration::seconds(30);

    #[test]
    fn held_lease_cannot_be_acquired() {
        let mut slot = LeaseSlot::default();
        slot.acquire("a", at(0), TTL).unwrap();
        assert!(matches!(
            slot.acquire("b", at(10), TTL),
            Err(LeaseError::Held { .. })
        ));
        // not even after expiry: only the reaper frees a slot
        assert!(matches!(
            slot.acquire("b", at(31), TTL),
            Err(LeaseError::Held { .. })
        ));
    }

    #[test]
    fn heartbeat_extends_lease_and_reaper_leaves_it_alone() {
        let mut slot = LeaseSlot::default();
        let l = slot.acquire("a", at(0), TTL).unwrap();
        slot.renew(l.epoch, at(20), TTL).unwrap();
        assert!(!slot.reap(at(40)), "renewed at 20, expires at 50");
        assert!(slot.reap(at(50)));
    }

    #[test]
    fn crashed_worker_is_reclaimed_under_a_new_epoch_and_fenced_out() {
        let mut slot = LeaseSlot::default();
        let a = slot.acquire("a", at(0), TTL).unwrap();
        // worker a crashes (no heartbeats); the lease expires and is reaped
        assert!(slot.reap(at(31)));
        let b = slot.acquire("b", at(32), TTL).unwrap();
        assert_eq!(b.epoch, a.epoch + 1);
        // a wakes up: heartbeat and completion are both rejected
        assert!(matches!(
            slot.renew(a.epoch, at(33), TTL),
            Err(LeaseError::Fenced { .. })
        ));
        assert!(matches!(
            slot.complete(a.epoch),
            Err(LeaseError::Fenced { .. })
        ));
        // b's completion is the one that counts
        slot.complete(b.epoch).unwrap();
        assert!(slot.holder().is_none());
    }

    #[test]
    fn missed_heartbeat_loses_lease_even_before_reap() {
        let mut slot = LeaseSlot::default();
        let a = slot.acquire("a", at(0), TTL).unwrap();
        assert_eq!(slot.renew(a.epoch, at(30), TTL), Err(LeaseError::Expired));
    }

    #[test]
    fn completing_twice_is_rejected() {
        let mut slot = LeaseSlot::default();
        let a = slot.acquire("a", at(0), TTL).unwrap();
        slot.complete(a.epoch).unwrap();
        assert!(slot.complete(a.epoch).is_err());
    }
}
