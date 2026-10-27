//! The execution harness loop: claim a step, heartbeat its lease while dbt runs,
//! stop dbt if the run is cancelled or the lease is lost, then record the result
//! under the claimed epoch.

use std::time::Duration;

use anyhow::Result;
use orch_core::retry::AttemptOutcome;
use orch_core::DbtCommand;
use tokio::sync::watch;
use tracing::Instrument;

use crate::executor::{self, ExecutorConfig, Stop};
use crate::store::{AttemptReport, ClaimedStep, Completion, Heartbeat, Store};

#[derive(Clone)]
pub struct Worker {
    pub store: Store,
    pub exec: ExecutorConfig,
    pub id: String,
    pub lease_ttl: Duration,
    pub poll_interval: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ran {
    Idle,
    Recorded(orch_core::StepStatus),
    /// Lease was lost or fenced; the result was discarded.
    Discarded,
}

impl Worker {
    pub async fn run_once(&self) -> Result<Ran> {
        let Some(claim) = self.store.claim_step(&self.id, self.lease_ttl).await? else {
            return Ok(Ran::Idle);
        };
        let span = tracing::info_span!("attempt", run_id = %claim.run_id, step = %claim.step_name,
            attempt = claim.attempt_no, epoch = claim.epoch, worker = %self.id);
        self.execute_claim(claim).instrument(span).await
    }

    pub async fn execute_claim(&self, claim: ClaimedStep) -> Result<Ran> {
        tracing::info!(command = %claim.command, "attempt started");
        let (stop_tx, stop_rx) = watch::channel(Stop::Running);
        let hb = tokio::spawn(heartbeat_loop(
            self.store.clone(),
            claim.step_id,
            claim.epoch,
            self.lease_ttl,
            stop_tx,
        ));

        let exec = match DbtCommand::parse(&claim.command) {
            Ok(cmd) => {
                let dir = format!("{}/{}-{}", claim.run_id, claim.step_name, claim.attempt_no);
                executor::execute(&self.exec, &cmd, &dir, claim.timeout, stop_rx).await
            }
            Err(e) => executor::Execution {
                outcome: AttemptOutcome::InfraError,
                exit_code: None,
                error: Some(e.to_string()),
                log_tail: String::new(),
                results: None,
            },
        };
        hb.abort();

        if exec.outcome == AttemptOutcome::Lost {
            tracing::warn!("lease lost mid-attempt; result discarded, the reaper will retry");
            return Ok(Ran::Discarded);
        }
        let report = AttemptReport {
            outcome: exec.outcome,
            exit_code: exec.exit_code,
            error: exec.error,
            log_tail: exec.log_tail,
            nodes: exec.results.map(|r| r.nodes).unwrap_or_default(),
        };
        match self.store.complete_attempt(&claim, &report).await? {
            Completion::Recorded(status) => {
                tracing::info!(
                    outcome = report.outcome.as_str(),
                    step_status = status.as_str(),
                    nodes = report.nodes.len(),
                    "attempt recorded"
                );
                Ok(Ran::Recorded(status))
            }
            Completion::Fenced => {
                tracing::warn!("stale lease epoch at completion; result discarded");
                Ok(Ran::Discarded)
            }
        }
    }

    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        tracing::info!(worker = %self.id, "worker started");
        while !*shutdown.borrow() {
            match self.run_once().await {
                Ok(Ran::Idle) => {}
                Ok(_) => continue,
                Err(e) => tracing::error!(error = %e, "worker iteration failed"),
            }
            tokio::select! {
                _ = tokio::time::sleep(self.poll_interval) => {}
                _ = shutdown.changed() => {}
            }
        }
        Ok(())
    }
}

/// Renew every ttl/3 so two missed heartbeats still leave margin. A transient DB
/// error is retried on the next beat; only a definite `Lost` stops dbt.
async fn heartbeat_loop(
    store: Store,
    step_id: uuid::Uuid,
    epoch: i64,
    ttl: Duration,
    stop: watch::Sender<Stop>,
) {
    let every = ttl / 3;
    loop {
        tokio::time::sleep(every).await;
        match store.heartbeat(step_id, epoch, ttl).await {
            Ok(Heartbeat::Held {
                cancel_requested: false,
            }) => {}
            Ok(Heartbeat::Held {
                cancel_requested: true,
            }) => {
                tracing::info!("cancel requested; stopping dbt");
                let _ = stop.send(Stop::Cancel);
                return;
            }
            Ok(Heartbeat::Lost) => {
                let _ = stop.send(Stop::LeaseLost);
                return;
            }
            Err(e) => tracing::warn!(error = %e, "heartbeat failed; will retry"),
        }
    }
}
