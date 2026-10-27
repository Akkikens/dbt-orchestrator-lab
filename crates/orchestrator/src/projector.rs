//! Postgres results projection. The `processed_events` insert and the projection
//! writes share one transaction: either the event is recorded as processed *and*
//! its effects are applied, or neither. That is what makes redelivery harmless.
//! The status rule itself is `orch_core::projection::merge_status`.

use anyhow::Result;
use orch_core::events::{EventKind, RunEvent};
use orch_core::projection::{merge_status, status_for, Applied};
use orch_core::RunStatus;
use sqlx::{PgPool, Row};

pub async fn apply(pool: &PgPool, ev: &RunEvent) -> Result<Applied> {
    let mut tx = pool.begin().await?;
    let first_time =
        sqlx::query("INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING")
            .bind(ev.event_id)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1;
    if !first_time {
        tx.commit().await?;
        return Ok(Applied::Duplicate);
    }

    let current = sqlx::query("SELECT status FROM run_history WHERE run_id = $1 FOR UPDATE")
        .bind(ev.run_id)
        .fetch_optional(&mut *tx)
        .await?
        .and_then(|r| RunStatus::parse(r.get::<String, _>("status").as_str()))
        .unwrap_or(RunStatus::Queued);
    let status = status_for(&ev.kind).map_or(current, |s| merge_status(current, s));

    let at = Some(ev.occurred_at);
    let (queued_at, started_at, finished_at) = match &ev.kind {
        EventKind::Queued => (at, None, None),
        EventKind::Started => (None, at, None),
        EventKind::Succeeded | EventKind::Failed { .. } | EventKind::Cancelled => (None, None, at),
        EventKind::NodeCompleted { .. } => (None, None, None),
    };
    let (execs, fails) = match &ev.kind {
        EventKind::NodeCompleted { node, .. } => (1i64, node.status.is_failure() as i64),
        _ => (0, 0),
    };
    sqlx::query(
        "INSERT INTO run_history (run_id, job_name, status, queued_at, started_at, finished_at, node_executions, node_failures)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (run_id) DO UPDATE SET
           status = EXCLUDED.status,
           queued_at = COALESCE(run_history.queued_at, EXCLUDED.queued_at),
           started_at = COALESCE(run_history.started_at, EXCLUDED.started_at),
           finished_at = COALESCE(run_history.finished_at, EXCLUDED.finished_at),
           node_executions = run_history.node_executions + EXCLUDED.node_executions,
           node_failures = run_history.node_failures + EXCLUDED.node_failures,
           updated_at = now()",
    )
    .bind(ev.run_id)
    .bind(&ev.job_name)
    .bind(status.as_str())
    .bind(queued_at)
    .bind(started_at)
    .bind(finished_at)
    .bind(execs)
    .bind(fails)
    .execute(&mut *tx)
    .await?;

    if let EventKind::NodeCompleted {
        step,
        attempt,
        node,
    } = &ev.kind
    {
        sqlx::query(
            "INSERT INTO node_results (run_id, step_name, attempt_no, unique_id, resource_type, status,
                                       execution_time_s, started_at, completed_at, message, failures)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT DO NOTHING",
        )
        .bind(ev.run_id)
        .bind(step)
        .bind(*attempt as i32)
        .bind(&node.unique_id)
        .bind(&node.resource_type)
        .bind(node.status.as_str())
        .bind(node.execution_time_s)
        .bind(node.started_at)
        .bind(node.completed_at)
        .bind(&node.message)
        .bind(node.failures)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(Applied::New)
}
