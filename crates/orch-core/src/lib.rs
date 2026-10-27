//! Pure logic for the dbt orchestrator: job DAGs, cron evaluation, retry/backoff,
//! the lease protocol, the run planner, `run_results.json` parsing and the event
//! model. Nothing here does I/O, so every rule is unit-testable without Postgres,
//! Kafka or dbt. The `orchestrator` crate applies these rules inside SQL
//! transactions.

pub mod command;
pub mod events;
pub mod job;
pub mod lease;
pub mod planner;
pub mod projection;
pub mod retry;
pub mod run_results;
pub mod schedule;

pub use command::DbtCommand;
pub use events::{EventKind, RunEvent};
pub use job::{JobSpec, JobsFile, StepSpec};
pub use planner::{RunStatus, StepStatus};
pub use retry::{AttemptOutcome, RetryDecision, RetryPolicy};
pub use run_results::{NodeResult, NodeStatus, ParsedRunResults};
