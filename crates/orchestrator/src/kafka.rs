//! Kafka plumbing (rdkafka / librdkafka). Tested against Redpanda in CI.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::types::RDKafkaErrorCode;
use tokio::sync::watch;

use crate::projector;
use crate::relay::Publisher;
use crate::store::Store;

pub async fn ensure_topic(brokers: &str, topic: &str, partitions: i32) -> Result<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .context("creating Kafka admin client")?;
    let res = admin
        .create_topics(
            &[NewTopic::new(topic, partitions, TopicReplication::Fixed(1))],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
        )
        .await?;
    for r in res {
        match r {
            Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((t, e)) => return Err(anyhow!("creating topic {t}: {e}")),
        }
    }
    Ok(())
}

pub struct KafkaPublisher {
    producer: FutureProducer,
    topic: String,
}

impl KafkaPublisher {
    pub fn new(brokers: &str, topic: &str) -> Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            // idempotent producer: broker-side dedupe of librdkafka's own retries
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("message.timeout.ms", "15000")
            .create()
            .context("creating Kafka producer")?;
        Ok(Self {
            producer,
            topic: topic.to_string(),
        })
    }
}

impl Publisher for KafkaPublisher {
    async fn publish(&self, key: &str, payload: &str) -> Result<()> {
        self.producer
            .send(
                FutureRecord::to(&self.topic).key(key).payload(payload),
                Duration::from_secs(15),
            )
            .await
            .map_err(|(e, _)| anyhow!("kafka delivery failed: {e}"))?;
        Ok(())
    }
}

pub fn consumer(brokers: &str, group: &str, topic: &str) -> Result<StreamConsumer> {
    let c: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        // offsets are committed only after the projection transaction commits
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "10000")
        .create()
        .context("creating Kafka consumer")?;
    c.subscribe(&[topic])?;
    Ok(c)
}

/// Consume run events and project them. A message's offset is committed only
/// after its projection commits, so a crash replays it (at-least-once) and the
/// projector's dedupe absorbs the replay. A database error retries the same
/// message in place, holding its partition, rather than skipping it.
pub async fn run_projector(
    store: Store,
    consumer: StreamConsumer,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    tracing::info!("results projector started");
    loop {
        let msg = tokio::select! {
            m = consumer.recv() => m,
            _ = shutdown.changed() => return Ok(()),
        };
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "kafka receive error");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        let payload = msg.payload().unwrap_or_default();
        match serde_json::from_slice::<orch_core::RunEvent>(payload) {
            Ok(ev) => {
                let mut backoff = Duration::from_millis(100);
                loop {
                    match projector::apply(store.pool(), &ev).await {
                        Ok(applied) => {
                            tracing::debug!(event_id = %ev.event_id, kind = ev.kind.type_name(), ?applied, "projected");
                            break;
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "projection failed; retrying");
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(Duration::from_secs(5));
                        }
                    }
                }
            }
            // poison message: log and move on (a real system would dead-letter it)
            Err(e) => {
                tracing::error!(error = %e, offset = msg.offset(), "undecodable event skipped")
            }
        }
        if let Err(e) = consumer.commit_message(&msg, CommitMode::Async) {
            tracing::warn!(error = %e, "offset commit failed; message may be redelivered");
        }
    }
}
