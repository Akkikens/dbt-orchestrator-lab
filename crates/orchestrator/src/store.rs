//! Durable control-plane state in Postgres. Every state transition runs in one
//! transaction together with the outbox rows that announce it, and the decisions
//! inside those transactions (retry, skip, finish) come from `orch_core`.
//!
//! Lock order is always run row, then step rows. Anything that changes a step
//! and then re-plans its run (completion, reaping) takes the run lock first, so
//! two workers finishing sibling steps serialize instead of deadlocking.
//!
//! Lease times use the database clock (`now()`), never the worker's, so clock skew
//! between workers cannot make a live lease look expired.

use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use async_graphql::SimpleObject;
use chrono::{DateTime, Utc};
use orch_core::events::{EventKind, RunEvent};
use orch_core::planner::{self, StepView};
use orch_core::retry::{self, AttemptOutcome, RetryDecision};
use orch_core::schedule::{self, CronSchedule};
use orch_core::{JobSpec, NodeResult, RunStatus, StepStatus};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{Postgres, Row, Transaction};
use uuid::Uuid;

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trigger {
    Cron,
    Manual,
}

impl Trigger {
    fn as_str(self) -> &'static str {
        match self {
            Self::Cron => "cron",
            Self::Manual => "manual",
        }
    }
}

#[derive(Debug, Clone, sqlx::FromRow, SimpleObject)]
#[graphql(complex, name = "Run")]
pub struct RunRecord {
    pub id: Uuid,
    pub job_name: String,
    pub trigger: String,
    pub idempotency_key: String,
    pub status: String,
    pub cancel_requested: bool,
    pub scheduled_for: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, sqlx::FromRow, SimpleObject)]
#[graphql(complex, name = "Step")]
pub struct StepRecord {
    pub id: Uuid,
    pub run_id: Uuid,
    pub name: String,
    pub command: String,
    pub depends_on: Vec<String>,
    pub status: String,
    pub attempts: i32,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub lease_epoch: i64,
}

#[derive(Debug, Clone, sqlx::FromRow, SimpleObject)]
#[graphql(name = "Attempt")]
pub struct AttemptRecord {
    pub id: Uuid,
    pub step_id: Uuid,
    pub attempt_no: i32,
    pub worker: String,
    pub lease_epoch: i64,
    pub status: String,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
    pub log_tail: Option<String>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, sqlx::FromRow, SimpleObject)]
#[graphql(name = "RunHistory")]
pub struct RunHistoryRecord {
    pub run_id: Uuid,
    pub job_name: String,
    pub status: String,
    pub queued_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub node_executions: i64,
    pub node_failures: i64,
}

#[derive(Debug, Clone, sqlx::FromRow, SimpleObject)]
#[graphql(name = "NodeResult")]
pub struct NodeResultRecord {
    pub run_id: Uuid,
    pub step_name: String,
    pub attempt_no: i32,
    pub unique_id: String,
    pub resource_type: String,
    pub status: String,
    pub execution_time_s: f64,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub message: Option<String>,
    pub failures: Option<i64>,
}

/// A step handed to a worker under a lease.
#[derive(Debug, Clone)]
pub struct ClaimedStep {
    pub step_id: Uuid,
    pub run_id: Uuid,
    pub job_name: String,
    pub step_name: String,
    pub command: String,
    pub attempt_no: u32,
    pub epoch: i64,
    pub timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct AttemptReport {
    pub outcome: AttemptOutcome,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
    pub log_tail: String,
    pub nodes: Vec<NodeResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Heartbeat {
    Held { cancel_requested: bool },
    Lost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completion {
    Recorded(StepStatus),
    /// The lease was reclaimed (stale epoch); nothing was written.
    Fenced,
}

const RUN_COLS: &str = "id, job_name, trigger, idempotency_key, status, cancel_requested, scheduled_for, created_at, started_at, finished_at";
const STEP_COLS: &str = "id, run_id, name, command, depends_on, status, attempts, next_attempt_at, lease_owner, lease_expires_at, lease_epoch";

#[derive(Clone)]
pub struct Store {
    pool: PgPool,
}

type Tx<'a> = Transaction<'a, Postgres>;

fn ms(d: Duration) -> i64 {
    d.as_millis().min(i64::MAX as u128) as i64
}

fn jitter_seed(step_id: Uuid) -> u64 {
    let b = step_id.as_bytes();
    u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
}

async fn insert_event(tx: &mut Tx<'_>, ev: &RunEvent) -> Result<()> {
    sqlx::query(
        "INSERT INTO outbox (event_id, run_id, payload) VALUES ($1, $2, $3) ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(ev.event_id)
    .bind(ev.run_id)
    .bind(serde_json::to_value(ev)?)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

impl Store {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .acquire_timeout(Duration::from_secs(10))
            .connect(url)
            .await
            .with_context(|| "connecting to Postgres")?;
        Ok(Self { pool })
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn migrate(&self) -> Result<()> {
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    // ---- jobs ------------------------------------------------------------------

    pub async fn upsert_job(&self, spec: &JobSpec) -> Result<()> {
        spec.validate()?;
        sqlx::query(
            "INSERT INTO jobs (name, spec, cron, max_concurrency) VALUES ($1, $2, $3, $4)
             ON CONFLICT (name) DO UPDATE
               SET spec = EXCLUDED.spec, cron = EXCLUDED.cron,
                   max_concurrency = EXCLUDED.max_concurrency, updated_at = now()",
        )
        .bind(&spec.name)
        .bind(serde_json::to_value(spec)?)
        .bind(&spec.cron)
        .bind(spec.max_concurrency as i32)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn jobs(&self) -> Result<Vec<JobSpec>> {
        let rows = sqlx::query("SELECT spec FROM jobs ORDER BY name")
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(|r| Ok(serde_json::from_value(r.get("spec"))?))
            .collect()
    }

    // ---- triggering --------------------------------------------------------------

    /// Create a run, or return the existing one for this idempotency key.
    /// Returns `(run_id, created)`.
    pub async fn trigger_run(
        &self,
        job: &str,
        trigger: Trigger,
        idempotency_key: &str,
    ) -> Result<(Uuid, bool)> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query("SELECT spec FROM jobs WHERE name = $1")
            .bind(job)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| anyhow!("unknown job `{job}`"))?;
        let spec: JobSpec = serde_json::from_value(row.get("spec"))?;
        let out = Self::insert_run_tx(&mut tx, &spec, trigger, idempotency_key, None).await?;
        tx.commit().await?;
        Ok(out)
    }

    async fn insert_run_tx(
        tx: &mut Tx<'_>,
        spec: &JobSpec,
        trigger: Trigger,
        key: &str,
        scheduled_for: Option<DateTime<Utc>>,
    ) -> Result<(Uuid, bool)> {
        let id = Uuid::new_v4();
        let inserted = sqlx::query(
            "INSERT INTO runs (id, job_name, spec, trigger, idempotency_key, status, scheduled_for)
             VALUES ($1, $2, $3, $4, $5, 'queued', $6)
             ON CONFLICT (idempotency_key) DO NOTHING RETURNING created_at",
        )
        .bind(id)
        .bind(&spec.name)
        .bind(serde_json::to_value(spec)?)
        .bind(trigger.as_str())
        .bind(key)
        .bind(scheduled_for)
        .fetch_optional(&mut **tx)
        .await?;
        let Some(row) = inserted else {
            let existing = sqlx::query("SELECT id, job_name FROM runs WHERE idempotency_key = $1")
                .bind(key)
                .fetch_one(&mut **tx)
                .await?;
            let existing_job: String = existing.get("job_name");
            if existing_job != spec.name {
                bail!("idempotency key `{key}` already used by job `{existing_job}`");
            }
            return Ok((existing.get("id"), false));
        };
        for s in &spec.steps {
            sqlx::query(
                "INSERT INTO steps (id, run_id, name, command, depends_on, status) VALUES ($1, $2, $3, $4, $5, 'pending')",
            )
            .bind(Uuid::new_v4())
            .bind(id)
            .bind(&s.name)
            .bind(&s.command)
            .bind(&s.depends_on)
            .execute(&mut **tx)
            .await?;
        }
        insert_event(
            tx,
            &RunEvent::new(id, &spec.name, row.get("created_at"), EventKind::Queued),
        )
        .await?;
        Ok((id, true))
    }

    /// One cron pass. Each job is handled in its own transaction holding the job
    /// row lock, so concurrent schedulers serialize per job; the idempotency key
    /// makes a repeated fire a no-op even without the lock.
    pub async fn schedule_cron(&self, now: DateTime<Utc>) -> Result<Vec<Uuid>> {
        let names: Vec<String> =
            sqlx::query_scalar("SELECT name FROM jobs WHERE cron IS NOT NULL ORDER BY name")
                .fetch_all(&self.pool)
                .await?;
        let mut fired = Vec::new();
        for name in names {
            let mut tx = self.pool.begin().await?;
            let row =
                sqlx::query("SELECT spec, last_scheduled_at FROM jobs WHERE name = $1 FOR UPDATE")
                    .bind(&name)
                    .fetch_one(&mut *tx)
                    .await?;
            let spec: JobSpec = serde_json::from_value(row.get("spec"))?;
            let Some(expr) = spec.cron.as_deref() else {
                continue;
            };
            let sched = CronSchedule::parse(expr).map_err(|e| anyhow!(e))?;
            let d = schedule::decide_tick(&sched, row.get("last_scheduled_at"), now);
            if let Some(at) = d.fire_at {
                let key = schedule::cron_idempotency_key(&name, at);
                let (id, created) =
                    Self::insert_run_tx(&mut tx, &spec, Trigger::Cron, &key, Some(at)).await?;
                if created {
                    tracing::info!(job = %name, run_id = %id, fire_at = %at, "cron fired");
                    fired.push(id);
                }
            }
            sqlx::query("UPDATE jobs SET last_scheduled_at = $2 WHERE name = $1")
                .bind(&name)
                .bind(d.watermark)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
        }
        Ok(fired)
    }

    // ---- admission and planning --------------------------------------------------

    /// Move queued runs to running, oldest first, within each job's concurrency
    /// limit. The job row lock makes the count-then-admit atomic.
    pub async fn admit_runs(&self) -> Result<Vec<Uuid>> {
        let jobs: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT job_name FROM runs WHERE status = 'queued' AND NOT cancel_requested",
        )
        .fetch_all(&self.pool)
        .await?;
        let mut admitted = Vec::new();
        for job in jobs {
            let mut tx = self.pool.begin().await?;
            let max: i32 =
                sqlx::query_scalar("SELECT max_concurrency FROM jobs WHERE name = $1 FOR UPDATE")
                    .bind(&job)
                    .fetch_one(&mut *tx)
                    .await?;
            let running: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM runs WHERE job_name = $1 AND status = 'running'",
            )
            .bind(&job)
            .fetch_one(&mut *tx)
            .await?;
            let queued: Vec<Uuid> = sqlx::query_scalar(
                "SELECT id FROM runs WHERE job_name = $1 AND status = 'queued' AND NOT cancel_requested
                 ORDER BY created_at, id",
            )
            .bind(&job)
            .fetch_all(&mut *tx)
            .await?;
            let n = planner::admit_count(max as u32, running as u32, queued.len() as u32) as usize;
            for id in queued.into_iter().take(n) {
                let started: DateTime<Utc> = sqlx::query_scalar(
                    "UPDATE runs SET status = 'running', started_at = now() WHERE id = $1 RETURNING started_at",
                )
                .bind(id)
                .fetch_one(&mut *tx)
                .await?;
                insert_event(
                    &mut tx,
                    &RunEvent::new(id, &job, started, EventKind::Started),
                )
                .await?;
                Self::advance_run_tx(&mut tx, id).await?;
                admitted.push(id);
            }
            tx.commit().await?;
        }
        Ok(admitted)
    }

    /// Re-plan one run: queue steps whose dependencies succeeded, skip or cancel
    /// steps that can no longer run, and finish the run when every step is done.
    async fn advance_run_tx(tx: &mut Tx<'_>, run_id: Uuid) -> Result<Option<RunStatus>> {
        let run = sqlx::query(
            "SELECT job_name, status, cancel_requested FROM runs WHERE id = $1 FOR UPDATE",
        )
        .bind(run_id)
        .fetch_one(&mut **tx)
        .await?;
        let status = RunStatus::parse(run.get::<String, _>("status").as_str())
            .ok_or_else(|| anyhow!("bad run status"))?;
        let cancel: bool = run.get("cancel_requested");
        let job: String = run.get("job_name");
        if status.is_terminal() || (status == RunStatus::Queued && !cancel) {
            return Ok(None);
        }
        let steps = sqlx::query(
            "SELECT name, depends_on, status FROM steps WHERE run_id = $1 ORDER BY name FOR UPDATE",
        )
        .bind(run_id)
        .fetch_all(&mut **tx)
        .await?;
        let views: Vec<StepView> = steps
            .iter()
            .map(|r| {
                Ok(StepView {
                    name: r.get("name"),
                    depends_on: r.get("depends_on"),
                    status: StepStatus::parse(r.get::<String, _>("status").as_str())
                        .ok_or_else(|| anyhow!("bad step status"))?,
                })
            })
            .collect::<Result<_>>()?;
        let plan = planner::plan(&views, cancel);
        for (names, to) in [
            (&plan.queue, "queued"),
            (&plan.skip, "skipped"),
            (&plan.cancel, "cancelled"),
        ] {
            if names.is_empty() {
                continue;
            }
            sqlx::query(
                "UPDATE steps SET status = $3, next_attempt_at = CASE WHEN $3 = 'queued' THEN now() ELSE next_attempt_at END
                 WHERE run_id = $1 AND name = ANY($2)",
            )
            .bind(run_id)
            .bind(names)
            .bind(to)
            .execute(&mut **tx)
            .await?;
        }
        if let Some(outcome) = plan.outcome {
            let finished: DateTime<Utc> = sqlx::query_scalar(
                "UPDATE runs SET status = $2, finished_at = now() WHERE id = $1 RETURNING finished_at",
            )
            .bind(run_id)
            .bind(outcome.as_str())
            .fetch_one(&mut **tx)
            .await?;
            let kind = match outcome {
                RunStatus::Succeeded => EventKind::Succeeded,
                RunStatus::Cancelled => EventKind::Cancelled,
                _ => {
                    let failed: Vec<&str> = views
                        .iter()
                        .filter(|v| v.status == StepStatus::Failed)
                        .map(|v| v.name.as_str())
                        .collect();
                    EventKind::Failed {
                        reason: Some(format!("failed steps: {}", failed.join(", "))),
                    }
                }
            };
            insert_event(tx, &RunEvent::new(run_id, &job, finished, kind)).await?;
            tracing::info!(%run_id, job = %job, status = outcome.as_str(), "run finished");
        }
        Ok(plan.outcome)
    }

    /// Safety net for the scheduler tick: re-plan every active run.
    pub async fn advance_active_runs(&self) -> Result<()> {
        let ids: Vec<Uuid> = sqlx::query_scalar(
            "SELECT id FROM runs WHERE status = 'running' OR (status = 'queued' AND cancel_requested)",
        )
        .fetch_all(&self.pool)
        .await?;
        for id in ids {
            let mut tx = self.pool.begin().await?;
            Self::advance_run_tx(&mut tx, id).await?;
            tx.commit().await?;
        }
        Ok(())
    }

    pub async fn request_cancel(&self, run_id: Uuid) -> Result<Option<RunRecord>> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("UPDATE runs SET cancel_requested = true WHERE id = $1 AND status IN ('queued', 'running')")
            .bind(run_id)
            .execute(&mut *tx)
            .await?;
        if sqlx::query("SELECT 1 FROM runs WHERE id = $1")
            .bind(run_id)
            .fetch_optional(&mut *tx)
            .await?
            .is_some()
        {
            Self::advance_run_tx(&mut tx, run_id).await?;
        }
        tx.commit().await?;
        self.run(run_id).await
    }

    // ---- leases ------------------------------------------------------------------

    /// Claim the oldest runnable step. `SKIP LOCKED` lets many workers poll the
    /// same table without blocking each other; the epoch bump is the fencing token.
    pub async fn claim_step(&self, worker: &str, ttl: Duration) -> Result<Option<ClaimedStep>> {
        let mut tx = self.pool.begin().await?;
        let Some(row) = sqlx::query(
            "WITH c AS (
               SELECT s.id FROM steps s JOIN runs r ON r.id = s.run_id
               WHERE s.status = 'queued' AND s.next_attempt_at <= now()
                 AND r.status = 'running' AND NOT r.cancel_requested
               ORDER BY s.next_attempt_at, s.id
               LIMIT 1
               FOR UPDATE OF s SKIP LOCKED
             )
             UPDATE steps s
                SET status = 'running', attempts = s.attempts + 1, lease_owner = $1,
                    lease_expires_at = now() + $2 * interval '1 millisecond',
                    lease_epoch = s.lease_epoch + 1
               FROM c, runs r
              WHERE s.id = c.id AND r.id = s.run_id
          RETURNING s.id, s.run_id, s.name, s.command, s.attempts, s.lease_epoch, r.job_name, r.spec",
        )
        .bind(worker)
        .bind(ms(ttl))
        .fetch_optional(&mut *tx)
        .await?
        else {
            return Ok(None);
        };
        let spec: JobSpec = serde_json::from_value(row.get("spec"))?;
        let claimed = ClaimedStep {
            step_id: row.get("id"),
            run_id: row.get("run_id"),
            job_name: row.get("job_name"),
            step_name: row.get("name"),
            command: row.get("command"),
            attempt_no: row.get::<i32, _>("attempts") as u32,
            epoch: row.get("lease_epoch"),
            timeout: Duration::from_secs(spec.timeout_secs),
        };
        sqlx::query(
            "INSERT INTO attempts (id, step_id, attempt_no, worker, lease_epoch, status) VALUES ($1, $2, $3, $4, $5, 'running')",
        )
        .bind(Uuid::new_v4())
        .bind(claimed.step_id)
        .bind(claimed.attempt_no as i32)
        .bind(worker)
        .bind(claimed.epoch)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Some(claimed))
    }

    /// Extend the lease. Fails (Lost) on a stale epoch or an already-expired lease;
    /// also reports whether the run was asked to cancel.
    pub async fn heartbeat(&self, step_id: Uuid, epoch: i64, ttl: Duration) -> Result<Heartbeat> {
        let row = sqlx::query(
            "UPDATE steps s SET lease_expires_at = now() + $3 * interval '1 millisecond'
               FROM runs r
              WHERE s.id = $1 AND s.lease_epoch = $2 AND s.status = 'running'
                AND s.lease_expires_at > now() AND r.id = s.run_id
          RETURNING r.cancel_requested",
        )
        .bind(step_id)
        .bind(epoch)
        .bind(ms(ttl))
        .fetch_optional(&self.pool)
        .await?;
        Ok(match row {
            Some(r) => Heartbeat::Held {
                cancel_requested: r.get("cancel_requested"),
            },
            None => Heartbeat::Lost,
        })
    }

    /// Record an attempt's result if (and only if) the caller still holds the
    /// lease epoch it claimed with. Node events go to the outbox in the same
    /// transaction, so results are announced exactly when they are committed.
    pub async fn complete_attempt(
        &self,
        claim: &ClaimedStep,
        report: &AttemptReport,
    ) -> Result<Completion> {
        let mut tx = self.pool.begin().await?;
        let run = sqlx::query("SELECT spec, cancel_requested FROM runs WHERE id = $1 FOR UPDATE")
            .bind(claim.run_id)
            .fetch_one(&mut *tx)
            .await?;
        let fenced = sqlx::query(
            "UPDATE steps SET lease_owner = NULL, lease_expires_at = NULL
              WHERE id = $1 AND lease_epoch = $2 AND status = 'running' RETURNING id",
        )
        .bind(claim.step_id)
        .bind(claim.epoch)
        .fetch_optional(&mut *tx)
        .await?
        .is_none();
        if fenced {
            tx.rollback().await?;
            return Ok(Completion::Fenced);
        }
        let now: DateTime<Utc> = sqlx::query_scalar(
            "UPDATE attempts SET status = $3, exit_code = $4, error = $5, log_tail = $6, finished_at = now()
              WHERE step_id = $1 AND attempt_no = $2 RETURNING finished_at",
        )
        .bind(claim.step_id)
        .bind(claim.attempt_no as i32)
        .bind(report.outcome.as_str())
        .bind(report.exit_code)
        .bind(&report.error)
        .bind(&report.log_tail)
        .fetch_one(&mut *tx)
        .await?;
        for node in &report.nodes {
            let ev = RunEvent::new(
                claim.run_id,
                &claim.job_name,
                node.completed_at.unwrap_or(now),
                EventKind::NodeCompleted {
                    step: claim.step_name.clone(),
                    attempt: claim.attempt_no,
                    node: node.clone(),
                },
            );
            insert_event(&mut tx, &ev).await?;
        }
        let spec: JobSpec = serde_json::from_value(run.get("spec"))?;
        let status = Self::settle_step_tx(
            &mut tx,
            claim.step_id,
            &spec,
            claim.attempt_no,
            report.outcome,
            run.get("cancel_requested"),
        )
        .await?;
        Self::advance_run_tx(&mut tx, claim.run_id).await?;
        tx.commit().await?;
        Ok(Completion::Recorded(status))
    }

    /// Apply the retry policy to a finished attempt and set the step's status.
    async fn settle_step_tx(
        tx: &mut Tx<'_>,
        step_id: Uuid,
        spec: &JobSpec,
        attempt_no: u32,
        outcome: AttemptOutcome,
        cancel_requested: bool,
    ) -> Result<StepStatus> {
        let decision = if cancel_requested && outcome != AttemptOutcome::Succeeded {
            RetryDecision::GiveUp
        } else {
            retry::decide(&spec.retry, attempt_no, outcome, jitter_seed(step_id))
        };
        let (status, delay) = match decision {
            RetryDecision::Done => (StepStatus::Succeeded, None),
            RetryDecision::RetryAfter(d) => (StepStatus::Queued, Some(d)),
            RetryDecision::GiveUp if cancel_requested || outcome == AttemptOutcome::Cancelled => {
                (StepStatus::Cancelled, None)
            }
            RetryDecision::GiveUp => (StepStatus::Failed, None),
        };
        sqlx::query(
            "UPDATE steps SET status = $2,
                    next_attempt_at = CASE WHEN $3::bigint IS NULL THEN next_attempt_at
                                           ELSE now() + $3 * interval '1 millisecond' END
              WHERE id = $1",
        )
        .bind(step_id)
        .bind(status.as_str())
        .bind(delay.map(ms))
        .execute(&mut **tx)
        .await?;
        if let Some(d) = delay {
            tracing::info!(%step_id, attempt_no, outcome = outcome.as_str(), delay_ms = ms(d), "retry scheduled");
        }
        Ok(status)
    }

    /// Reclaim steps whose worker stopped heartbeating: mark the attempt `lost`
    /// and let the retry policy requeue or fail the step. Returns how many.
    pub async fn reap_expired_leases(&self) -> Result<usize> {
        let expired = sqlx::query(
            "SELECT id, run_id, lease_epoch, attempts FROM steps
              WHERE status = 'running' AND lease_expires_at <= now() ORDER BY lease_expires_at LIMIT 100",
        )
        .fetch_all(&self.pool)
        .await?;
        let mut reaped = 0;
        for r in expired {
            let (step_id, run_id): (Uuid, Uuid) = (r.get("id"), r.get("run_id"));
            let (epoch, attempt_no): (i64, i32) = (r.get("lease_epoch"), r.get("attempts"));
            let mut tx = self.pool.begin().await?;
            let run =
                sqlx::query("SELECT spec, cancel_requested FROM runs WHERE id = $1 FOR UPDATE")
                    .bind(run_id)
                    .fetch_one(&mut *tx)
                    .await?;
            // re-check under lock: the worker may have completed or heartbeated since
            let still_expired = sqlx::query(
                "UPDATE steps SET lease_owner = NULL, lease_expires_at = NULL
                  WHERE id = $1 AND lease_epoch = $2 AND status = 'running' AND lease_expires_at <= now()
                  RETURNING id",
            )
            .bind(step_id)
            .bind(epoch)
            .fetch_optional(&mut *tx)
            .await?
            .is_some();
            if !still_expired {
                tx.rollback().await?;
                continue;
            }
            sqlx::query(
                "UPDATE attempts SET status = 'lost', error = 'lease expired (worker stopped heartbeating)', finished_at = now()
                  WHERE step_id = $1 AND attempt_no = $2",
            )
            .bind(step_id)
            .bind(attempt_no)
            .execute(&mut *tx)
            .await?;
            let spec: JobSpec = serde_json::from_value(run.get("spec"))?;
            Self::settle_step_tx(
                &mut tx,
                step_id,
                &spec,
                attempt_no as u32,
                AttemptOutcome::Lost,
                run.get("cancel_requested"),
            )
            .await?;
            Self::advance_run_tx(&mut tx, run_id).await?;
            tx.commit().await?;
            tracing::warn!(%step_id, %run_id, attempt_no, "reclaimed expired lease");
            reaped += 1;
        }
        Ok(reaped)
    }

    // ---- outbox ------------------------------------------------------------------

    /// Lock up to `limit` unpublished outbox rows in id order. The caller publishes
    /// them and commits via [`Store::mark_published`] in the same transaction.
    pub async fn begin_outbox_batch(
        &self,
        limit: i64,
    ) -> Result<(Tx<'static>, Vec<(i64, Uuid, String)>)> {
        let mut tx = self.pool.begin().await?;
        let rows = sqlx::query(
            "SELECT id, run_id, payload::text AS payload FROM outbox WHERE published_at IS NULL
              ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED",
        )
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;
        let batch = rows
            .into_iter()
            .map(|r| (r.get("id"), r.get("run_id"), r.get("payload")))
            .collect();
        Ok((tx, batch))
    }

    pub async fn mark_published(mut tx: Tx<'static>, ids: &[i64]) -> Result<()> {
        sqlx::query("UPDATE outbox SET published_at = now() WHERE id = ANY($1)")
            .bind(ids)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    // ---- reads -------------------------------------------------------------------

    pub async fn run(&self, id: Uuid) -> Result<Option<RunRecord>> {
        Ok(
            sqlx::query_as(&format!("SELECT {RUN_COLS} FROM runs WHERE id = $1"))
                .bind(id)
                .fetch_optional(&self.pool)
                .await?,
        )
    }

    pub async fn runs(&self, job: Option<&str>, limit: i64) -> Result<Vec<RunRecord>> {
        Ok(sqlx::query_as(&format!(
            "SELECT {RUN_COLS} FROM runs WHERE ($1::text IS NULL OR job_name = $1)
              ORDER BY created_at DESC, id LIMIT $2"
        ))
        .bind(job)
        .bind(limit.clamp(1, 500))
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn steps(&self, run_id: Uuid) -> Result<Vec<StepRecord>> {
        Ok(sqlx::query_as(&format!(
            "SELECT {STEP_COLS} FROM steps WHERE run_id = $1 ORDER BY name"
        ))
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn attempts(&self, step_id: Uuid) -> Result<Vec<AttemptRecord>> {
        Ok(sqlx::query_as(
            "SELECT id, step_id, attempt_no, worker, lease_epoch, status, exit_code, error, log_tail, started_at, finished_at
               FROM attempts WHERE step_id = $1 ORDER BY attempt_no",
        )
        .bind(step_id)
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn run_history(&self, run_id: Uuid) -> Result<Option<RunHistoryRecord>> {
        Ok(sqlx::query_as(
            "SELECT run_id, job_name, status, queued_at, started_at, finished_at, node_executions, node_failures
               FROM run_history WHERE run_id = $1",
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn node_results(
        &self,
        run_id: Uuid,
        status: Option<&str>,
    ) -> Result<Vec<NodeResultRecord>> {
        Ok(sqlx::query_as(
            "SELECT run_id, step_name, attempt_no, unique_id, resource_type, status, execution_time_s,
                    started_at, completed_at, message, failures
               FROM node_results WHERE run_id = $1 AND ($2::text IS NULL OR status = $2)
              ORDER BY step_name, attempt_no, started_at NULLS LAST, unique_id",
        )
        .bind(run_id)
        .bind(status)
        .fetch_all(&self.pool)
        .await?)
    }
}
