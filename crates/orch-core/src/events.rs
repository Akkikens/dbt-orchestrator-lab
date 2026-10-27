//! Run lifecycle events published to Kafka. Event ids are **deterministic**
//! (UUIDv5 of the run id and what happened), not random. If the same transition
//! is recorded twice, or the outbox relay republishes after a crash, the copies
//! share an id, and the projector discards all but the first.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::run_results::NodeResult;

/// Namespace for event ids (a fixed random UUID; changing it changes every id).
const EVENT_NAMESPACE: Uuid = Uuid::from_u128(0x6b1d_2f0c_5a8e_4c3b_9f71_d0a4_e2c6_1b37);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunEvent {
    pub event_id: Uuid,
    pub run_id: Uuid,
    pub job_name: String,
    pub occurred_at: DateTime<Utc>,
    #[serde(flatten)]
    pub kind: EventKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventKind {
    Queued,
    Started,
    NodeCompleted {
        step: String,
        attempt: u32,
        node: NodeResult,
    },
    Succeeded,
    Failed {
        reason: Option<String>,
    },
    Cancelled,
}

impl EventKind {
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Started => "started",
            Self::NodeCompleted { .. } => "node_completed",
            Self::Succeeded => "succeeded",
            Self::Failed { .. } => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// What makes this event unique within its run. All terminal kinds share one
    /// discriminator, so a run can have at most one terminal event.
    fn discriminator(&self) -> String {
        match self {
            Self::Queued => "queued".into(),
            Self::Started => "started".into(),
            Self::NodeCompleted {
                step,
                attempt,
                node,
            } => format!("node/{step}/{attempt}/{}", node.unique_id),
            Self::Succeeded | Self::Failed { .. } | Self::Cancelled => "terminal".into(),
        }
    }
}

impl RunEvent {
    pub fn new(run_id: Uuid, job_name: &str, occurred_at: DateTime<Utc>, kind: EventKind) -> Self {
        let event_id = Uuid::new_v5(
            &EVENT_NAMESPACE,
            format!("{run_id}/{}", kind.discriminator()).as_bytes(),
        );
        Self {
            event_id,
            run_id,
            job_name: job_name.to_string(),
            occurred_at,
            kind,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_results::NodeStatus;

    fn node(id: &str) -> NodeResult {
        NodeResult {
            unique_id: id.into(),
            resource_type: "model".into(),
            status: NodeStatus::Success,
            execution_time_s: 0.1,
            started_at: None,
            completed_at: None,
            message: None,
            failures: None,
        }
    }

    #[test]
    fn ids_are_deterministic_and_ignore_timestamps() {
        let run = Uuid::new_v4();
        let a = RunEvent::new(run, "j", Utc::now(), EventKind::Started);
        let b = RunEvent::new(
            run,
            "j",
            Utc::now() + chrono::Duration::seconds(5),
            EventKind::Started,
        );
        assert_eq!(a.event_id, b.event_id);
        let other_run = RunEvent::new(Uuid::new_v4(), "j", Utc::now(), EventKind::Started);
        assert_ne!(a.event_id, other_run.event_id);
    }

    #[test]
    fn node_events_differ_by_step_attempt_and_node() {
        let run = Uuid::new_v4();
        let ev = |step: &str, attempt, id: &str| {
            RunEvent::new(
                run,
                "j",
                Utc::now(),
                EventKind::NodeCompleted {
                    step: step.into(),
                    attempt,
                    node: node(id),
                },
            )
            .event_id
        };
        let base = ev("build", 1, "model.x.a");
        assert_eq!(base, ev("build", 1, "model.x.a"));
        assert_ne!(
            base,
            ev("build", 2, "model.x.a"),
            "a retry is a new execution"
        );
        assert_ne!(base, ev("test", 1, "model.x.a"));
        assert_ne!(base, ev("build", 1, "model.x.b"));
    }

    #[test]
    fn a_run_has_one_terminal_event_id() {
        let run = Uuid::new_v4();
        let ok = RunEvent::new(run, "j", Utc::now(), EventKind::Succeeded);
        let failed = RunEvent::new(run, "j", Utc::now(), EventKind::Failed { reason: None });
        assert_eq!(ok.event_id, failed.event_id);
    }

    #[test]
    fn wire_format_is_flat_json_with_type_tag() {
        let run = Uuid::nil();
        let e = RunEvent::new(
            run,
            "nightly",
            Utc::now(),
            EventKind::NodeCompleted {
                step: "build".into(),
                attempt: 1,
                node: node("model.x.a"),
            },
        );
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["type"], "node_completed");
        assert_eq!(v["node"]["status"], "success");
        assert_eq!(v["job_name"], "nightly");
        let back: RunEvent = serde_json::from_value(v).unwrap();
        assert_eq!(back, e);
    }
}
