//! The scheduler tick. Each step is idempotent and safe to run from several
//! replicas at once (row locks + idempotency keys), so a tick that crashes halfway
//! is simply repeated.

use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use tokio::sync::watch;

use crate::store::Store;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TickReport {
    pub fired: usize,
    pub reaped: usize,
    pub admitted: usize,
}

pub async fn tick(store: &Store) -> Result<TickReport> {
    let fired = store.schedule_cron(Utc::now()).await?.len();
    let reaped = store.reap_expired_leases().await?;
    let admitted = store.admit_runs().await?.len();
    store.advance_active_runs().await?;
    Ok(TickReport {
        fired,
        reaped,
        admitted,
    })
}

pub async fn run(store: Store, every: Duration, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    tracing::info!(tick_ms = every.as_millis() as u64, "scheduler started");
    while !*shutdown.borrow() {
        match tick(&store).await {
            Ok(r) if r != TickReport::default() => tracing::info!(?r, "tick"),
            Ok(_) => {}
            Err(e) => tracing::error!(error = %e, "scheduler tick failed"),
        }
        tokio::select! {
            _ = tokio::time::sleep(every) => {}
            _ = shutdown.changed() => {}
        }
    }
    Ok(())
}
