//! Outbox relay: publish committed outbox rows to Kafka in id order and mark them
//! published. The mark happens after the broker acks, in the transaction that
//! locked the rows, so a crash between publish and mark republishes the batch:
//! **at-least-once**, with duplicates removed by the projector's event-id dedupe.

use std::future::Future;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::watch;

use crate::store::Store;

pub trait Publisher: Send + Sync {
    /// Resolve only once the broker has durably accepted the message.
    fn publish(&self, key: &str, payload: &str) -> impl Future<Output = Result<()>> + Send;
}

pub async fn relay_once<P: Publisher>(store: &Store, publisher: &P, batch: i64) -> Result<usize> {
    let (tx, rows) = store.begin_outbox_batch(batch).await?;
    if rows.is_empty() {
        return Ok(0);
    }
    let mut ids = Vec::with_capacity(rows.len());
    for (id, run_id, payload) in &rows {
        // keyed by run so every event of a run lands on one partition, in order
        publisher.publish(&run_id.to_string(), payload).await?;
        ids.push(*id);
    }
    Store::mark_published(tx, &ids).await?;
    Ok(ids.len())
}

pub async fn run<P: Publisher>(
    store: Store,
    publisher: P,
    every: Duration,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    tracing::info!("outbox relay started");
    while !*shutdown.borrow() {
        match relay_once(&store, &publisher, 500).await {
            Ok(n) if n > 0 => {
                tracing::debug!(published = n, "relayed outbox batch");
                continue;
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "relay failed; batch will be retried"),
        }
        tokio::select! {
            _ = tokio::time::sleep(every) => {}
            _ = shutdown.changed() => {}
        }
    }
    Ok(())
}
