//! Parsing dbt's `target/run_results.json` into per-node results. Only the fields
//! the orchestrator needs are read and unknown fields are ignored, so the parser
//! tolerates minor schema changes between dbt versions. Fixtures under
//! `tests/fixtures/` are real dbt-duckdb 1.12 output (compiled SQL stripped).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeStatus {
    Success,
    Pass,
    Warn,
    Error,
    Fail,
    Skipped,
    NoOp,
    Other(String),
}

impl NodeStatus {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Success => "success",
            Self::Pass => "pass",
            Self::Warn => "warn",
            Self::Error => "error",
            Self::Fail => "fail",
            Self::Skipped => "skipped",
            Self::NoOp => "no-op",
            Self::Other(s) => s,
        }
    }

    pub fn is_failure(&self) -> bool {
        matches!(self, Self::Error | Self::Fail)
    }
}

impl From<&str> for NodeStatus {
    fn from(s: &str) -> Self {
        match s {
            "success" => Self::Success,
            "pass" => Self::Pass,
            "warn" => Self::Warn,
            "error" | "runtime error" => Self::Error,
            "fail" => Self::Fail,
            "skipped" => Self::Skipped,
            "no-op" => Self::NoOp,
            other => Self::Other(other.to_string()),
        }
    }
}

impl Serialize for NodeStatus {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for NodeStatus {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Ok(NodeStatus::from(s.as_str()))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeResult {
    pub unique_id: String,
    /// `model`, `test`, `seed`, `snapshot`, ... (prefix of the unique id)
    pub resource_type: String,
    pub status: NodeStatus,
    pub execution_time_s: f64,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub message: Option<String>,
    /// Rows returned by a failing test; `None` for models and seeds.
    pub failures: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedRunResults {
    pub dbt_version: Option<String>,
    pub invocation_id: Option<String>,
    pub elapsed_time_s: f64,
    pub nodes: Vec<NodeResult>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Summary {
    pub total: usize,
    pub ok: usize,
    pub warned: usize,
    pub failed: usize,
    pub skipped: usize,
}

impl ParsedRunResults {
    pub fn summary(&self) -> Summary {
        let mut s = Summary {
            total: self.nodes.len(),
            ..Default::default()
        };
        for n in &self.nodes {
            match n.status {
                NodeStatus::Success | NodeStatus::Pass | NodeStatus::NoOp => s.ok += 1,
                NodeStatus::Warn => s.warned += 1,
                NodeStatus::Error | NodeStatus::Fail => s.failed += 1,
                NodeStatus::Skipped => s.skipped += 1,
                NodeStatus::Other(_) => {}
            }
        }
        s
    }
}

#[derive(Deserialize)]
struct Raw {
    #[serde(default)]
    metadata: RawMeta,
    results: Vec<RawNode>,
    #[serde(default)]
    elapsed_time: f64,
}

#[derive(Deserialize, Default)]
struct RawMeta {
    dbt_version: Option<String>,
    invocation_id: Option<String>,
}

#[derive(Deserialize)]
struct RawNode {
    unique_id: String,
    status: NodeStatus,
    #[serde(default)]
    timing: Vec<RawTiming>,
    #[serde(default)]
    execution_time: f64,
    message: Option<String>,
    failures: Option<i64>,
}

#[derive(Deserialize)]
struct RawTiming {
    name: String,
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
}

pub fn parse(json: &str) -> Result<ParsedRunResults, serde_json::Error> {
    let raw: Raw = serde_json::from_str(json)?;
    let nodes = raw
        .results
        .into_iter()
        .map(|n| {
            // "execute" is the timing that matters; fall back to the first entry
            let t = n
                .timing
                .iter()
                .find(|t| t.name == "execute")
                .or(n.timing.first());
            NodeResult {
                resource_type: n
                    .unique_id
                    .split('.')
                    .next()
                    .unwrap_or_default()
                    .to_string(),
                status: n.status,
                execution_time_s: n.execution_time,
                started_at: t.and_then(|t| t.started_at),
                completed_at: t.and_then(|t| t.completed_at),
                message: n.message,
                failures: n.failures,
                unique_id: n.unique_id,
            }
        })
        .collect();
    Ok(ParsedRunResults {
        dbt_version: raw.metadata.dbt_version,
        invocation_id: raw.metadata.invocation_id,
        elapsed_time_s: raw.elapsed_time,
        nodes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SUCCESS: &str = include_str!("../tests/fixtures/run_results_success.json");
    const TEST_FAILURE: &str = include_str!("../tests/fixtures/run_results_test_failure.json");
    const MODEL_ERROR: &str = include_str!("../tests/fixtures/run_results_model_error.json");

    #[test]
    fn parses_successful_build() {
        let r = parse(SUCCESS).unwrap();
        assert_eq!(r.dbt_version.as_deref(), Some("1.12.6"));
        let s = r.summary();
        assert_eq!(
            s,
            Summary {
                total: 13,
                ok: 13,
                warned: 0,
                failed: 0,
                skipped: 0
            }
        );
        let seed = &r.nodes[0];
        assert_eq!(seed.unique_id, "seed.jaffle.raw_customers");
        assert_eq!(seed.resource_type, "seed");
        assert_eq!(seed.status, NodeStatus::Success);
        assert!(seed.started_at.unwrap() <= seed.completed_at.unwrap());
        let test = r.nodes.iter().find(|n| n.resource_type == "test").unwrap();
        assert_eq!(
            (test.status.clone(), test.failures),
            (NodeStatus::Pass, Some(0))
        );
    }

    #[test]
    fn parses_failing_test_with_failure_count() {
        let r = parse(TEST_FAILURE).unwrap();
        assert_eq!(r.nodes.len(), 1);
        let n = &r.nodes[0];
        assert_eq!(n.unique_id, "test.jaffle.always_fails");
        assert_eq!(n.status, NodeStatus::Fail);
        assert!(n.status.is_failure());
        assert_eq!(n.failures, Some(5));
        assert!(n.message.as_deref().unwrap().contains("Got 5 results"));
    }

    #[test]
    fn parses_model_error_and_skipped_downstream() {
        let r = parse(MODEL_ERROR).unwrap();
        let s = r.summary();
        assert_eq!((s.failed, s.skipped, s.ok), (1, 7, 5));
        let err = r
            .nodes
            .iter()
            .find(|n| n.status == NodeStatus::Error)
            .unwrap();
        assert_eq!(err.unique_id, "model.jaffle.stg_orders");
        assert!(err.message.as_deref().unwrap().contains("Binder Error"));
    }

    #[test]
    fn tolerates_unknown_fields_and_statuses_and_rejects_garbage() {
        let r = parse(
            r#"{"results":[{"unique_id":"model.x.y","status":"partial success","extra":1}]}"#,
        )
        .unwrap();
        assert_eq!(
            r.nodes[0].status,
            NodeStatus::Other("partial success".into())
        );
        assert_eq!(r.nodes[0].started_at, None);
        assert!(parse("{\"results\": 3}").is_err());
        assert!(parse("not json").is_err());
    }

    #[test]
    fn node_status_serializes_as_dbt_string() {
        let n = &parse(TEST_FAILURE).unwrap().nodes[0];
        let v = serde_json::to_value(n).unwrap();
        assert_eq!(v["status"], "fail");
        let back: NodeResult = serde_json::from_value(v).unwrap();
        assert_eq!(&back, n);
    }
}
