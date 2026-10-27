//! The results projection's rules, shared by the in-memory projector (used in
//! tests) and the Postgres projector in `orchestrator::projector`.
//!
//! Delivery is at-least-once, so the same event can arrive many times and, after
//! a relay retry, out of order with respect to its neighbours. Two rules make the
//! projection correct anyway:
//! 1. **Dedupe by event id.** An event id is applied once; counters such as
//!    `node_executions` would otherwise double-count on redelivery.
//! 2. **Status only moves forward.** `queued < running < terminal`, and a terminal
//!    status is never replaced, so a late `started` cannot reopen a finished run.

use std::collections::{BTreeMap, HashMap, HashSet};

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::events::{EventKind, RunEvent};
use crate::planner::RunStatus;
use crate::run_results::NodeResult;

fn rank(s: RunStatus) -> u8 {
    match s {
        RunStatus::Queued => 0,
        RunStatus::Running => 1,
        RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled => 2,
    }
}

pub fn merge_status(current: RunStatus, incoming: RunStatus) -> RunStatus {
    if current.is_terminal() || rank(incoming) <= rank(current) {
        current
    } else {
        incoming
    }
}

/// The run status an event implies, if any.
pub fn status_for(kind: &EventKind) -> Option<RunStatus> {
    match kind {
        EventKind::Queued => Some(RunStatus::Queued),
        EventKind::Started => Some(RunStatus::Running),
        EventKind::NodeCompleted { .. } => None,
        EventKind::Succeeded => Some(RunStatus::Succeeded),
        EventKind::Failed { .. } => Some(RunStatus::Failed),
        EventKind::Cancelled => Some(RunStatus::Cancelled),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RunRow {
    pub job_name: String,
    pub status: RunStatus,
    pub queued_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub node_executions: u64,
    pub node_failures: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Applied {
    New,
    Duplicate,
}

/// Node results keyed by (run, step, attempt, node).
pub type NodeKey = (Uuid, String, u32, String);

#[derive(Debug, Default)]
pub struct Projector {
    seen: HashSet<Uuid>,
    pub runs: HashMap<Uuid, RunRow>,
    pub nodes: BTreeMap<NodeKey, NodeResult>,
}

impl Projector {
    pub fn apply(&mut self, ev: &RunEvent) -> Applied {
        if !self.seen.insert(ev.event_id) {
            return Applied::Duplicate;
        }
        let row = self.runs.entry(ev.run_id).or_insert_with(|| RunRow {
            job_name: ev.job_name.clone(),
            status: RunStatus::Queued,
            queued_at: None,
            started_at: None,
            finished_at: None,
            node_executions: 0,
            node_failures: 0,
        });
        if let Some(s) = status_for(&ev.kind) {
            row.status = merge_status(row.status, s);
        }
        match &ev.kind {
            EventKind::Queued => row.queued_at = Some(ev.occurred_at),
            EventKind::Started => row.started_at = Some(ev.occurred_at),
            EventKind::NodeCompleted {
                step,
                attempt,
                node,
            } => {
                row.node_executions += 1;
                if node.status.is_failure() {
                    row.node_failures += 1;
                }
                self.nodes.insert(
                    (ev.run_id, step.clone(), *attempt, node.unique_id.clone()),
                    node.clone(),
                );
            }
            EventKind::Succeeded | EventKind::Failed { .. } | EventKind::Cancelled => {
                row.finished_at.get_or_insert(ev.occurred_at);
            }
        }
        Applied::New
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_results::NodeStatus;

    fn node_ev(run: Uuid, attempt: u32, id: &str, status: NodeStatus) -> RunEvent {
        RunEvent::new(
            run,
            "j",
            Utc::now(),
            EventKind::NodeCompleted {
                step: "build".into(),
                attempt,
                node: NodeResult {
                    unique_id: id.into(),
                    resource_type: "test".into(),
                    status,
                    execution_time_s: 0.0,
                    started_at: None,
                    completed_at: None,
                    message: None,
                    failures: Some(1),
                },
            },
        )
    }

    #[test]
    fn redelivered_events_are_applied_once() {
        let run = Uuid::new_v4();
        let events = [
            RunEvent::new(run, "j", Utc::now(), EventKind::Queued),
            RunEvent::new(run, "j", Utc::now(), EventKind::Started),
            node_ev(run, 1, "test.x.a", NodeStatus::Fail),
            node_ev(run, 1, "model.x.b", NodeStatus::Success),
            RunEvent::new(run, "j", Utc::now(), EventKind::Failed { reason: None }),
        ];
        let mut once = Projector::default();
        for e in &events {
            assert_eq!(once.apply(e), Applied::New);
        }
        // deliver everything three more times, in reverse order
        let mut many = Projector::default();
        for e in &events {
            many.apply(e);
        }
        for _ in 0..3 {
            for e in events.iter().rev() {
                assert_eq!(many.apply(e), Applied::Duplicate);
            }
        }
        assert_eq!(once.runs, many.runs);
        assert_eq!(once.nodes, many.nodes);
        let row = &many.runs[&run];
        assert_eq!((row.node_executions, row.node_failures), (2, 1));
        assert_eq!(row.status, RunStatus::Failed);
    }

    #[test]
    fn same_node_in_a_retry_is_a_separate_execution() {
        let run = Uuid::new_v4();
        let mut p = Projector::default();
        p.apply(&node_ev(run, 1, "test.x.a", NodeStatus::Fail));
        p.apply(&node_ev(run, 2, "test.x.a", NodeStatus::Pass));
        assert_eq!(p.runs[&run].node_executions, 2);
        assert_eq!(p.runs[&run].node_failures, 1);
        assert_eq!(p.nodes.len(), 2);
    }

    #[test]
    fn status_never_moves_backwards() {
        use RunStatus::*;
        assert_eq!(merge_status(Queued, Running), Running);
        assert_eq!(merge_status(Running, Queued), Running);
        assert_eq!(merge_status(Running, Succeeded), Succeeded);
        assert_eq!(merge_status(Succeeded, Running), Succeeded);
        assert_eq!(merge_status(Failed, Succeeded), Failed, "terminal sticks");
        let run = Uuid::new_v4();
        let mut p = Projector::default();
        p.apply(&RunEvent::new(run, "j", Utc::now(), EventKind::Succeeded));
        p.apply(&RunEvent::new(run, "j", Utc::now(), EventKind::Started)); // late
        assert_eq!(p.runs[&run].status, Succeeded);
        assert!(
            p.runs[&run].started_at.is_some(),
            "timestamps still fill in"
        );
    }
}
