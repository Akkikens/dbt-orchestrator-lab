use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use orchestrator::executor::ExecutorConfig;
use orchestrator::store::{Store, Trigger};
use orchestrator::worker::Worker;
use orchestrator::{api, kafka, relay, scheduler, telemetry};
use tokio::sync::watch;

#[derive(Parser)]
#[command(name = "orchestrator", about = "dbt job orchestrator (portfolio lab)")]
struct Cli {
    #[arg(
        long,
        env = "DATABASE_URL",
        global = true,
        default_value = "postgres://postgres:postgres@localhost:5432/orchestrator"
    )]
    database_url: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Apply database migrations.
    Migrate,
    /// Validate a jobs file and upsert its jobs.
    LoadJobs {
        #[arg(long, env = "JOBS_FILE", default_value = "jobs.toml")]
        file: PathBuf,
    },
    /// Trigger a manual run (idempotent per key).
    Trigger {
        job: String,
        #[arg(long)]
        key: Option<String>,
    },
    Scheduler(SchedulerArgs),
    Worker(WorkerArgs),
    Relay(KafkaArgs),
    Projector(KafkaArgs),
    Api(ApiArgs),
    /// Everything in one process: migrate, load jobs, then scheduler + worker +
    /// relay + projector + API.
    All {
        #[arg(long, env = "JOBS_FILE", default_value = "jobs.toml")]
        jobs_file: PathBuf,
        #[command(flatten)]
        scheduler: SchedulerArgs,
        #[command(flatten)]
        worker: WorkerArgs,
        #[command(flatten)]
        kafka: KafkaArgs,
        #[command(flatten)]
        api: ApiArgs,
    },
}

#[derive(Args, Clone)]
struct SchedulerArgs {
    #[arg(long, env = "TICK_MS", default_value_t = 1000)]
    tick_ms: u64,
}

#[derive(Args, Clone)]
struct WorkerArgs {
    #[arg(long, env = "WORKER_ID")]
    worker_id: Option<String>,
    #[arg(long, env = "WORKER_SLOTS", default_value_t = 1)]
    slots: usize,
    #[arg(long, env = "LEASE_TTL_SECS", default_value_t = 30)]
    lease_ttl_secs: u64,
    #[arg(long, env = "DBT_BIN", default_value = "dbt")]
    dbt_bin: PathBuf,
    #[arg(long, env = "DBT_PROJECT_DIR", default_value = "examples/jaffle")]
    project_dir: PathBuf,
    #[arg(long, env = "WORK_DIR", default_value = "/tmp/dbt-orchestrator")]
    work_dir: PathBuf,
    #[arg(
        long,
        env = "DBT_DUCKDB_PATH",
        default_value = "/tmp/dbt-orchestrator/jaffle.duckdb"
    )]
    duckdb_path: String,
}

#[derive(Args, Clone)]
struct KafkaArgs {
    #[arg(long, env = "KAFKA_BROKERS", default_value = "localhost:9092")]
    brokers: String,
    #[arg(long, env = "KAFKA_TOPIC", default_value = "dbt.run.events")]
    topic: String,
    #[arg(long, env = "KAFKA_GROUP", default_value = "results-projector")]
    group: String,
}

#[derive(Args, Clone)]
struct ApiArgs {
    #[arg(long, env = "API_ADDR", default_value = "0.0.0.0:8080")]
    addr: String,
}

fn worker(store: &Store, a: &WorkerArgs, slot: usize) -> Result<Worker> {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "local".into());
    let base = a
        .worker_id
        .clone()
        .unwrap_or_else(|| format!("{host}-{}", std::process::id()));
    Ok(Worker {
        store: store.clone(),
        exec: ExecutorConfig {
            dbt_bin: a.dbt_bin.clone(),
            project_dir: std::path::absolute(&a.project_dir)?,
            work_dir: a.work_dir.clone(),
            env: vec![("DBT_DUCKDB_PATH".into(), a.duckdb_path.clone())],
            log_tail_bytes: 64 * 1024,
        },
        id: format!("{base}/{slot}"),
        lease_ttl: Duration::from_secs(a.lease_ttl_secs),
        poll_interval: Duration::from_millis(500),
    })
}

async fn serve_api(store: Store, a: ApiArgs, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(&a.addr).await?;
    tracing::info!(addr = %a.addr, "GraphQL API listening (GET /graphql for GraphiQL)");
    axum::serve(listener, api::router(api::schema(store)))
        .with_graceful_shutdown(async move {
            let _ = shutdown.changed().await;
        })
        .await?;
    Ok(())
}

fn shutdown_signal() -> watch::Receiver<bool> {
    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("shutting down");
        let _ = tx.send(true);
    });
    rx
}

#[tokio::main]
async fn main() -> Result<()> {
    let _guard = telemetry::init("dbt-orchestrator");
    let cli = Cli::parse();
    let store = Store::connect(&cli.database_url).await?;
    let stop = shutdown_signal();
    match cli.cmd {
        Cmd::Migrate => store.migrate().await?,
        Cmd::LoadJobs { file } => {
            for j in orchestrator::load_jobs(&file)?.jobs {
                store.upsert_job(&j).await?;
                tracing::info!(job = %j.name, "job loaded");
            }
        }
        Cmd::Trigger { job, key } => {
            let key = key.unwrap_or_else(|| format!("manual:{}", uuid::Uuid::new_v4()));
            let (id, created) = store.trigger_run(&job, Trigger::Manual, &key).await?;
            println!("{{\"run_id\":\"{id}\",\"created\":{created}}}");
        }
        Cmd::Scheduler(a) => scheduler::run(store, Duration::from_millis(a.tick_ms), stop).await?,
        Cmd::Worker(a) => run_workers(&store, &a, stop).await?,
        Cmd::Relay(k) => {
            kafka::ensure_topic(&k.brokers, &k.topic, 3).await?;
            relay::run(
                store,
                kafka::KafkaPublisher::new(&k.brokers, &k.topic)?,
                Duration::from_millis(200),
                stop,
            )
            .await?
        }
        Cmd::Projector(k) => {
            kafka::ensure_topic(&k.brokers, &k.topic, 3).await?;
            kafka::run_projector(
                store,
                kafka::consumer(&k.brokers, &k.group, &k.topic)?,
                stop,
            )
            .await?
        }
        Cmd::Api(a) => serve_api(store, a, stop).await?,
        Cmd::All {
            jobs_file,
            scheduler: s,
            worker: w,
            kafka: k,
            api: a,
        } => {
            store.migrate().await?;
            for j in orchestrator::load_jobs(&jobs_file)?.jobs {
                store.upsert_job(&j).await?;
            }
            kafka::ensure_topic(&k.brokers, &k.topic, 3).await?;
            let publisher = kafka::KafkaPublisher::new(&k.brokers, &k.topic)?;
            let consumer = kafka::consumer(&k.brokers, &k.group, &k.topic)?;
            tokio::try_join!(
                scheduler::run(
                    store.clone(),
                    Duration::from_millis(s.tick_ms),
                    stop.clone()
                ),
                run_workers(&store, &w, stop.clone()),
                relay::run(
                    store.clone(),
                    publisher,
                    Duration::from_millis(200),
                    stop.clone()
                ),
                kafka::run_projector(store.clone(), consumer, stop.clone()),
                serve_api(store.clone(), a, stop.clone()),
            )?;
        }
    }
    Ok(())
}

async fn run_workers(store: &Store, a: &WorkerArgs, stop: watch::Receiver<bool>) -> Result<()> {
    let mut handles = Vec::new();
    for slot in 0..a.slots.max(1) {
        handles.push(tokio::spawn(worker(store, a, slot)?.run(stop.clone())));
    }
    for h in handles {
        h.await??;
    }
    Ok(())
}
