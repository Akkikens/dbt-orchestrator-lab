//! GraphQL API (async-graphql on axum): trigger and cancel runs, read run state
//! from the control plane and node results from the Kafka-fed projection.

use async_graphql::http::GraphiQLSource;
use async_graphql::{
    ComplexObject, Context, EmptySubscription, Object, Result, Schema, SimpleObject,
};
use async_graphql_axum::GraphQL;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::Router;
use uuid::Uuid;

use crate::store::{
    AttemptRecord, NodeResultRecord, RunHistoryRecord, RunRecord, StepRecord, Store, Trigger,
};

pub type ApiSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

pub fn schema(store: Store) -> ApiSchema {
    Schema::build(QueryRoot, MutationRoot, EmptySubscription)
        .data(store)
        .limit_depth(8)
        .finish()
}

pub fn router(schema: ApiSchema) -> Router {
    Router::new()
        .route("/graphql", get(graphiql).post_service(GraphQL::new(schema)))
        .route("/healthz", get(|| async { "ok" }))
}

async fn graphiql() -> impl IntoResponse {
    Html(GraphiQLSource::build().endpoint("/graphql").finish())
}

fn store<'a>(ctx: &Context<'a>) -> &'a Store {
    ctx.data_unchecked::<Store>()
}

#[derive(SimpleObject)]
pub struct Job {
    name: String,
    cron: Option<String>,
    max_concurrency: u32,
    max_attempts: u32,
    timeout_secs: u64,
    steps: Vec<JobStep>,
}

#[derive(SimpleObject)]
pub struct JobStep {
    name: String,
    command: String,
    depends_on: Vec<String>,
}

#[derive(SimpleObject)]
pub struct TriggerResult {
    run: RunRecord,
    /// False when the idempotency key matched an existing run.
    created: bool,
}

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn jobs(&self, ctx: &Context<'_>) -> Result<Vec<Job>> {
        Ok(store(ctx)
            .jobs()
            .await?
            .into_iter()
            .map(|j| Job {
                name: j.name,
                cron: j.cron,
                max_concurrency: j.max_concurrency,
                max_attempts: j.retry.max_attempts,
                timeout_secs: j.timeout_secs,
                steps: j
                    .steps
                    .into_iter()
                    .map(|s| JobStep {
                        name: s.name,
                        command: s.command,
                        depends_on: s.depends_on,
                    })
                    .collect(),
            })
            .collect())
    }

    /// Most recent runs first.
    async fn runs(
        &self,
        ctx: &Context<'_>,
        job: Option<String>,
        #[graphql(default = 20)] limit: i64,
    ) -> Result<Vec<RunRecord>> {
        Ok(store(ctx).runs(job.as_deref(), limit).await?)
    }

    async fn run(&self, ctx: &Context<'_>, id: Uuid) -> Result<Option<RunRecord>> {
        Ok(store(ctx).run(id).await?)
    }
}

pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// Trigger a manual run. Passing the same `idempotencyKey` again returns the
    /// original run instead of starting a second one (safe client retries).
    async fn trigger_run(
        &self,
        ctx: &Context<'_>,
        job: String,
        idempotency_key: Option<String>,
    ) -> Result<TriggerResult> {
        let key = idempotency_key.unwrap_or_else(|| format!("manual:{}", Uuid::new_v4()));
        let s = store(ctx);
        let (id, created) = s.trigger_run(&job, Trigger::Manual, &key).await?;
        let run = s.run(id).await?.ok_or("run vanished")?;
        Ok(TriggerResult { run, created })
    }

    /// Request cancellation. Queued work is cancelled immediately; a running dbt
    /// process is killed by its worker on the next heartbeat.
    async fn cancel_run(&self, ctx: &Context<'_>, id: Uuid) -> Result<Option<RunRecord>> {
        Ok(store(ctx).request_cancel(id).await?)
    }
}

#[ComplexObject]
impl RunRecord {
    async fn steps(&self, ctx: &Context<'_>) -> Result<Vec<StepRecord>> {
        Ok(store(ctx).steps(self.id).await?)
    }

    /// Projected view of this run, built from Kafka events. Lags the control plane
    /// by the relay and consumer delay; `null` until the first event is projected.
    async fn history(&self, ctx: &Context<'_>) -> Result<Option<RunHistoryRecord>> {
        Ok(store(ctx).run_history(self.id).await?)
    }

    /// Per-node dbt results from the projection, optionally filtered by dbt status
    /// (`success`, `pass`, `fail`, `error`, `skipped`, ...).
    async fn node_results(
        &self,
        ctx: &Context<'_>,
        status: Option<String>,
    ) -> Result<Vec<NodeResultRecord>> {
        Ok(store(ctx).node_results(self.id, status.as_deref()).await?)
    }
}

#[ComplexObject]
impl StepRecord {
    async fn attempt_history(&self, ctx: &Context<'_>) -> Result<Vec<AttemptRecord>> {
        Ok(store(ctx).attempts(self.id).await?)
    }
}
