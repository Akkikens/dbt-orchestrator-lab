//! Integration tests against real Postgres, Kafka (Redpanda) and dbt-duckdb.
//!
//! Opt in with `ORCH_IT=1`. Without it every test returns immediately, so plain
//! `cargo test` passes on a laptop with no services. With it, each test
//! *requires* the services it uses and panics if an env var is missing, so CI
//! cannot go green by skipping.
//!
//! * `DATABASE_URL`  - a Postgres URL with CREATE DATABASE rights; each test makes its own database
//! * `KAFKA_BROKERS` - e.g. `localhost:9092`; each test makes its own topic
//! * `DBT_BIN`       - path to a `dbt` with the duckdb adapter installed

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant};

use orch_core::retry::RetryPolicy;
use orch_core::{JobSpec, RunStatus, StepSpec};
use orchestrator::executor::ExecutorConfig;
use orchestrator::store::{Completion, Heartbeat, RunRecord, Store, Trigger};
use orchestrator::worker::{Ran, Worker};
use orchestrator::{api, kafka, projector, relay, scheduler};
use rdkafka::consumer::Consumer;
use rdkafka::Message;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use uuid::Uuid;

macro_rules! require_it {
    () => {
        if std::env::var("ORCH_IT").as_deref() != Ok("1") {
            eprintln!("skipped: set ORCH_IT=1 to run integration tests");
            return;
        }
    };
}

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("ORCH_IT=1 but {name} is not set"))
}

const SUCCESS_FIXTURE: &str =
    include_str!("../../orch-core/tests/fixtures/run_results_success.json");

async fn fresh_store() -> Store {
    let admin = PgConnectOptions::from_str(&env("DATABASE_URL")).unwrap();
    let name = format!("it_{}", Uuid::new_v4().simple());
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(admin.clone())
        .await
        .unwrap();
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&admin_pool)
        .await
        .unwrap();
    admin_pool.close().await;
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect_with(admin.database(&name))
        .await
        .unwrap();
    let store = Store::from_pool(pool);
    store.migrate().await.unwrap();
    store
}

fn job(name: &str, steps: &[(&str, &str, &[&str])]) -> JobSpec {
    JobSpec {
        name: name.into(),
        cron: None,
        max_concurrency: 1,
        retry: RetryPolicy {
            max_attempts: 1,
            base_delay_ms: 50,
            max_delay_ms: 100,
            retry_on_node_failure: false,
        },
        timeout_secs: 120,
        steps: steps
            .iter()
            .map(|(n, c, d)| StepSpec {
                name: n.to_string(),
                command: c.to_string(),
                depends_on: d.iter().map(|s| s.to_string()).collect(),
            })
            .collect(),
    }
}

fn jaffle_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/jaffle")
        .canonicalize()
        .unwrap()
}

/// A worker running the real dbt binary against examples/jaffle, with its own
/// DuckDB file so parallel tests never contend for a database lock.
fn dbt_worker(store: &Store, id: &str, tmp: &Path, ttl: Duration) -> Worker {
    Worker {
        store: store.clone(),
        exec: ExecutorConfig {
            dbt_bin: env("DBT_BIN").into(),
            project_dir: jaffle_dir(),
            work_dir: tmp.join("work"),
            env: vec![(
                "DBT_DUCKDB_PATH".into(),
                tmp.join("jaffle.duckdb").display().to_string(),
            )],
            log_tail_bytes: 16 * 1024,
        },
        id: id.into(),
        lease_ttl: ttl,
        poll_interval: Duration::from_millis(50),
    }
}

/// A worker whose "dbt" is a shell script, for process-control scenarios
/// (slow, crashed) where real dbt would finish too quickly to observe.
fn fake_worker(store: &Store, id: &str, tmp: &Path, body: &str, ttl: Duration) -> Worker {
    std::fs::write(tmp.join("fixture.json"), SUCCESS_FIXTURE).unwrap();
    let script = tmp.join(format!("fake-dbt-{id}"));
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\nFIX='{}'\nwhile [ $# -gt 0 ]; do [ \"$1\" = --target-path ] && T=\"$2\"; shift; done\n{body}\n",
            tmp.join("fixture.json").display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    Worker {
        store: store.clone(),
        exec: ExecutorConfig {
            dbt_bin: script,
            project_dir: tmp.to_path_buf(),
            work_dir: tmp.join("work"),
            env: vec![],
            log_tail_bytes: 16 * 1024,
        },
        id: id.into(),
        lease_ttl: ttl,
        poll_interval: Duration::from_millis(50),
    }
}

async fn drive(store: &Store, workers: &[&Worker], run_id: Uuid, deadline: Duration) -> RunRecord {
    let t0 = Instant::now();
    loop {
        scheduler::tick(store).await.unwrap();
        for w in workers {
            w.run_once().await.unwrap();
        }
        let r = store.run(run_id).await.unwrap().unwrap();
        if RunStatus::parse(&r.status).unwrap().is_terminal() {
            return r;
        }
        assert!(
            t0.elapsed() < deadline,
            "run did not finish in {deadline:?}:\n{}",
            describe(store, run_id).await
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Run, steps and attempts (with errors and log tails) for assertion messages.
async fn describe(store: &Store, run_id: Uuid) -> String {
    let mut out = format!("{:?}\n", store.run(run_id).await.unwrap());
    for s in store.steps(run_id).await.unwrap() {
        out += &format!("  step {} {} attempts={}\n", s.name, s.status, s.attempts);
        for a in store.attempts(s.id).await.unwrap() {
            let tail = a.log_tail.unwrap_or_default();
            let tail: String = tail
                .lines()
                .rev()
                .take(15)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join("\n      ");
            out += &format!(
                "    attempt {} {} exit={:?} error={:?}\n      {tail}\n",
                a.attempt_no, a.status, a.exit_code, a.error
            );
        }
    }
    out
}

async fn outbox_types(store: &Store, run_id: Uuid) -> Vec<String> {
    sqlx::query_scalar("SELECT payload->>'type' FROM outbox WHERE run_id = $1 ORDER BY id")
        .bind(run_id)
        .fetch_all(store.pool())
        .await
        .unwrap()
}

// ---- scheduling -------------------------------------------------------------------

#[tokio::test]
async fn manual_trigger_is_idempotent_and_concurrency_limited() {
    require_it!();
    let store = fresh_store().await;
    store
        .upsert_job(&job("a", &[("s", "dbt build", &[])]))
        .await
        .unwrap();
    store
        .upsert_job(&job("b", &[("s", "dbt build", &[])]))
        .await
        .unwrap();

    let (r1, created) = store.trigger_run("a", Trigger::Manual, "k1").await.unwrap();
    assert!(created);
    let (again, created) = store.trigger_run("a", Trigger::Manual, "k1").await.unwrap();
    assert_eq!((again, created), (r1, false), "same key, same run");
    assert!(
        store.trigger_run("b", Trigger::Manual, "k1").await.is_err(),
        "key reused by another job"
    );

    store.trigger_run("a", Trigger::Manual, "k2").await.unwrap();
    store.trigger_run("a", Trigger::Manual, "k3").await.unwrap();
    scheduler::tick(&store).await.unwrap();
    scheduler::tick(&store).await.unwrap();
    let statuses: Vec<String> = store
        .runs(Some("a"), 10)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.status)
        .collect();
    assert_eq!(
        statuses.iter().filter(|s| *s == "running").count(),
        1,
        "max_concurrency = 1: {statuses:?}"
    );
    assert_eq!(statuses.iter().filter(|s| *s == "queued").count(), 2);
    assert_eq!(
        store.run(r1).await.unwrap().unwrap().status,
        "running",
        "oldest admitted first"
    );
    let steps = store.steps(r1).await.unwrap();
    assert_eq!(steps[0].status, "queued", "root step queued on admission");
}

#[tokio::test]
async fn cron_fires_once_per_window_even_with_a_second_scheduler() {
    require_it!();
    let store = fresh_store().await;
    let mut j = job("every_second", &[("s", "dbt build", &[])]);
    j.cron = Some("* * * * * *".into());
    store.upsert_job(&j).await.unwrap();

    let t0 = chrono::Utc::now();
    assert!(
        store.schedule_cron(t0).await.unwrap().is_empty(),
        "first sight only sets the watermark"
    );
    let t1 = t0 + chrono::Duration::seconds(3);
    assert_eq!(
        store.schedule_cron(t1).await.unwrap().len(),
        1,
        "3 missed seconds coalesce into one run"
    );
    assert!(store.schedule_cron(t1).await.unwrap().is_empty());

    // a second scheduler replica with a stale view of the watermark fires the same
    // tick; the idempotency key makes it a no-op
    sqlx::query("UPDATE jobs SET last_scheduled_at = $1")
        .bind(t0)
        .execute(store.pool())
        .await
        .unwrap();
    assert!(store.schedule_cron(t1).await.unwrap().is_empty());
    let runs = store.runs(Some("every_second"), 10).await.unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].trigger, "cron");
    assert!(runs[0].idempotency_key.starts_with("cron:every_second:"));
}

// ---- real dbt ----------------------------------------------------------------------

#[tokio::test]
async fn dbt_dag_runs_end_to_end() {
    require_it!();
    let store = fresh_store().await;
    let tmp = tempfile::tempdir().unwrap();
    store
        .upsert_job(&job(
            "jaffle",
            &[
                ("seed", "dbt seed", &[]),
                ("staging", "dbt build --select staging", &["seed"]),
                ("marts", "dbt build --select marts", &["staging"]),
            ],
        ))
        .await
        .unwrap();
    let (run_id, _) = store
        .trigger_run("jaffle", Trigger::Manual, "e2e")
        .await
        .unwrap();
    let w = dbt_worker(&store, "w1", tmp.path(), Duration::from_secs(30));
    let run = drive(&store, &[&w], run_id, Duration::from_secs(180)).await;
    assert_eq!(
        run.status,
        "succeeded",
        "{}",
        describe(&store, run_id).await
    );

    for s in store.steps(run_id).await.unwrap() {
        assert_eq!(
            (s.status.as_str(), s.attempts),
            ("succeeded", 1),
            "{}",
            s.name
        );
        let a = store.attempts(s.id).await.unwrap();
        assert_eq!(a[0].status, "succeeded");
        assert_eq!(a[0].exit_code, Some(0));
    }
    let types = outbox_types(&store, run_id).await;
    assert_eq!(types.first().map(String::as_str), Some("queued"));
    assert_eq!(types.get(1).map(String::as_str), Some("started"));
    assert_eq!(types.last().map(String::as_str), Some("succeeded"));
    // seed: 2 seeds; staging: 2 views + 6 tests; marts: 1 table + 2 tests
    assert_eq!(types.iter().filter(|t| *t == "node_completed").count(), 13);
}

#[tokio::test]
async fn failing_dbt_test_retries_then_skips_downstream() {
    require_it!();
    let store = fresh_store().await;
    let tmp = tempfile::tempdir().unwrap();
    let mut j = job(
        "broken",
        &[
            ("build", "dbt build", &[]),
            (
                "audit",
                "dbt test --select tag:always_fails --vars '{enable_failing_test: true}'",
                &["build"],
            ),
            ("publish", "dbt run --select marts", &["audit"]),
        ],
    );
    j.retry = RetryPolicy {
        max_attempts: 2,
        base_delay_ms: 100,
        max_delay_ms: 200,
        retry_on_node_failure: true,
    };
    store.upsert_job(&j).await.unwrap();
    let (run_id, _) = store
        .trigger_run("broken", Trigger::Manual, "broken-1")
        .await
        .unwrap();
    let w = dbt_worker(&store, "w1", tmp.path(), Duration::from_secs(30));
    let run = drive(&store, &[&w], run_id, Duration::from_secs(180)).await;
    assert_eq!(run.status, "failed");

    let steps = store.steps(run_id).await.unwrap();
    let by = |n: &str| steps.iter().find(|s| s.name == n).unwrap().clone();
    assert_eq!(by("build").status, "succeeded");
    assert_eq!(
        (by("audit").status.as_str(), by("audit").attempts),
        ("failed", 2)
    );
    assert_eq!(
        (by("publish").status.as_str(), by("publish").attempts),
        ("skipped", 0)
    );
    let attempts = store.attempts(by("audit").id).await.unwrap();
    assert!(attempts
        .iter()
        .all(|a| a.status == "node_failure" && a.exit_code == Some(1)));
    assert!(attempts[0]
        .error
        .as_deref()
        .unwrap()
        .contains("test.jaffle.always_fails"));

    let reason: String = sqlx::query_scalar(
        "SELECT payload->>'reason' FROM outbox WHERE run_id = $1 AND payload->>'type' = 'failed'",
    )
    .bind(run_id)
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert!(reason.contains("audit"));
}

// ---- leases, cancellation, timeouts --------------------------------------------------

#[tokio::test]
async fn crashed_worker_lease_is_reclaimed_and_zombie_is_fenced() {
    require_it!();
    let store = fresh_store().await;
    let tmp = tempfile::tempdir().unwrap();
    let mut j = job("lease", &[("s", "dbt build", &[])]);
    j.retry.max_attempts = 3;
    store.upsert_job(&j).await.unwrap();
    let (run_id, _) = store
        .trigger_run("lease", Trigger::Manual, "lease-1")
        .await
        .unwrap();
    scheduler::tick(&store).await.unwrap();

    // worker A claims and then "crashes": it never heartbeats or completes
    let ttl = Duration::from_millis(800);
    let zombie = store
        .claim_step("A", ttl)
        .await
        .unwrap()
        .expect("claimable");
    assert_eq!((zombie.attempt_no, zombie.epoch), (1, 1));
    assert!(
        store.claim_step("B", ttl).await.unwrap().is_none(),
        "leased step is not claimable"
    );

    tokio::time::sleep(ttl + Duration::from_millis(300)).await;
    assert_eq!(scheduler::tick(&store).await.unwrap().reaped, 1);
    let step = store.steps(run_id).await.unwrap().remove(0);
    assert_eq!((step.status.as_str(), step.attempts), ("queued", 1));
    assert_eq!(store.attempts(step.id).await.unwrap()[0].status, "lost");

    // worker B picks it up after the backoff and succeeds under epoch 2
    tokio::time::sleep(Duration::from_millis(200)).await;
    let b = fake_worker(
        &store,
        "B",
        tmp.path(),
        "cp \"$FIX\" \"$T/run_results.json\"",
        Duration::from_secs(5),
    );
    assert_eq!(
        b.run_once().await.unwrap(),
        Ran::Recorded(orch_core::StepStatus::Succeeded)
    );
    assert_eq!(store.steps(run_id).await.unwrap()[0].lease_epoch, 2);

    // A wakes up: its heartbeat and its result are both rejected
    assert_eq!(
        store
            .heartbeat(zombie.step_id, zombie.epoch, ttl)
            .await
            .unwrap(),
        Heartbeat::Lost
    );
    let report = orchestrator::store::AttemptReport {
        outcome: orch_core::AttemptOutcome::Succeeded,
        exit_code: Some(0),
        error: None,
        log_tail: String::new(),
        nodes: orch_core::run_results::parse(SUCCESS_FIXTURE)
            .unwrap()
            .nodes,
    };
    assert_eq!(
        store.complete_attempt(&zombie, &report).await.unwrap(),
        Completion::Fenced
    );

    assert_eq!(
        store.run(run_id).await.unwrap().unwrap().status,
        "succeeded"
    );
    let attempt1_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM outbox WHERE run_id = $1 AND payload->>'type' = 'node_completed' AND payload->>'attempt' = '1'",
    )
    .bind(run_id)
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(
        attempt1_events, 0,
        "the zombie's results never reached the outbox"
    );
}

#[tokio::test]
async fn cancel_kills_running_dbt_and_cancels_queued_runs() {
    require_it!();
    let store = fresh_store().await;
    let tmp = tempfile::tempdir().unwrap();
    let mut j = job(
        "slow",
        &[("s", "dbt build", &[]), ("after", "dbt build", &["s"])],
    );
    j.max_concurrency = 1;
    store.upsert_job(&j).await.unwrap();
    let (running, _) = store
        .trigger_run("slow", Trigger::Manual, "c1")
        .await
        .unwrap();
    let (queued, _) = store
        .trigger_run("slow", Trigger::Manual, "c2")
        .await
        .unwrap();
    scheduler::tick(&store).await.unwrap();

    // a queued run is cancelled on the spot
    let r = store.request_cancel(queued).await.unwrap().unwrap();
    assert_eq!(r.status, "cancelled");
    assert!(store
        .steps(queued)
        .await
        .unwrap()
        .iter()
        .all(|s| s.status == "cancelled"));

    // a running one: the worker sees the flag on its next heartbeat and kills dbt
    let w = fake_worker(
        &store,
        "w",
        tmp.path(),
        "sleep 60",
        Duration::from_millis(900),
    );
    let task = tokio::spawn(async move { w.run_once().await });
    let t0 = Instant::now();
    while store
        .steps(running)
        .await
        .unwrap()
        .iter()
        .all(|s| s.status != "running")
    {
        assert!(t0.elapsed() < Duration::from_secs(10));
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    store.request_cancel(running).await.unwrap();
    let ran = tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("dbt was not killed")
        .unwrap()
        .unwrap();
    assert_eq!(ran, Ran::Recorded(orch_core::StepStatus::Cancelled));
    assert!(
        t0.elapsed() < Duration::from_secs(15),
        "did not wait for sleep 60"
    );

    assert_eq!(
        store.run(running).await.unwrap().unwrap().status,
        "cancelled"
    );
    let steps = store.steps(running).await.unwrap();
    assert!(steps.iter().all(|s| s.status == "cancelled"), "{steps:?}");
    let s = steps.iter().find(|s| s.name == "s").unwrap();
    assert_eq!(store.attempts(s.id).await.unwrap()[0].status, "cancelled");
    assert_eq!(
        outbox_types(&store, running)
            .await
            .last()
            .map(String::as_str),
        Some("cancelled")
    );
}

#[tokio::test]
async fn timeouts_are_retried_with_backoff_then_fail() {
    require_it!();
    let store = fresh_store().await;
    let tmp = tempfile::tempdir().unwrap();
    let mut j = job("hangs", &[("s", "dbt build", &[])]);
    j.timeout_secs = 1;
    j.retry = RetryPolicy {
        max_attempts: 2,
        base_delay_ms: 100,
        max_delay_ms: 100,
        retry_on_node_failure: false,
    };
    store.upsert_job(&j).await.unwrap();
    let (run_id, _) = store
        .trigger_run("hangs", Trigger::Manual, "t1")
        .await
        .unwrap();
    let w = fake_worker(&store, "w", tmp.path(), "sleep 30", Duration::from_secs(5));
    let run = drive(&store, &[&w], run_id, Duration::from_secs(30)).await;
    assert_eq!(run.status, "failed");
    let step = store.steps(run_id).await.unwrap().remove(0);
    let attempts = store.attempts(step.id).await.unwrap();
    assert_eq!(
        attempts
            .iter()
            .map(|a| a.status.as_str())
            .collect::<Vec<_>>(),
        ["timed_out", "timed_out"]
    );
    let gap = attempts[1].started_at - attempts[0].finished_at.unwrap();
    assert!(
        gap >= chrono::Duration::milliseconds(50),
        "backoff respected: {gap}"
    );
}

// ---- Kafka + projection + API ---------------------------------------------------------

async fn topic() -> String {
    let t = format!("it-{}", Uuid::new_v4().simple());
    kafka::ensure_topic(&env("KAFKA_BROKERS"), &t, 1)
        .await
        .unwrap();
    t
}

#[tokio::test]
async fn redelivered_kafka_events_are_projected_once() {
    require_it!();
    let store = fresh_store().await;
    let tmp = tempfile::tempdir().unwrap();
    store
        .upsert_job(&job("jaffle", &[("build", "dbt build", &[])]))
        .await
        .unwrap();
    let (run_id, _) = store
        .trigger_run("jaffle", Trigger::Manual, "k")
        .await
        .unwrap();
    let w = dbt_worker(&store, "w1", tmp.path(), Duration::from_secs(30));
    assert_eq!(
        drive(&store, &[&w], run_id, Duration::from_secs(180))
            .await
            .status,
        "succeeded"
    );

    let brokers = env("KAFKA_BROKERS");
    let topic = topic().await;
    let publisher = kafka::KafkaPublisher::new(&brokers, &topic).unwrap();
    let n = relay::relay_once(&store, &publisher, 1000).await.unwrap();
    assert_eq!(n, 16, "queued + started + 13 nodes + succeeded");
    assert_eq!(
        relay::relay_once(&store, &publisher, 1000).await.unwrap(),
        0,
        "nothing left"
    );
    // simulate the relay crashing after Kafka acked but before it marked the rows:
    // the whole batch is published a second time
    sqlx::query("UPDATE outbox SET published_at = NULL")
        .execute(store.pool())
        .await
        .unwrap();
    assert_eq!(
        relay::relay_once(&store, &publisher, 1000).await.unwrap(),
        n
    );

    let consumer = kafka::consumer(&brokers, &format!("g-{}", Uuid::new_v4()), &topic).unwrap();
    let (mut new, mut dup) = (0, 0);
    while new + dup < 2 * n {
        let msg = tokio::time::timeout(Duration::from_secs(30), consumer.recv())
            .await
            .expect("message")
            .unwrap();
        let ev: orch_core::RunEvent = serde_json::from_slice(msg.payload().unwrap()).unwrap();
        assert_eq!(
            msg.key(),
            Some(run_id.to_string().as_bytes()),
            "keyed by run"
        );
        match projector::apply(store.pool(), &ev).await.unwrap() {
            orch_core::projection::Applied::New => new += 1,
            orch_core::projection::Applied::Duplicate => dup += 1,
        }
    }
    consumer.unsubscribe();
    assert_eq!((new, dup), (n, n));

    let h = store.run_history(run_id).await.unwrap().unwrap();
    assert_eq!(h.status, "succeeded");
    assert_eq!(
        (h.node_executions, h.node_failures),
        (13, 0),
        "counters not doubled"
    );
    assert!(h.queued_at.is_some() && h.started_at.is_some() && h.finished_at.is_some());
    assert_eq!(store.node_results(run_id, None).await.unwrap().len(), 13);
}

#[tokio::test]
async fn projector_loop_and_graphql_expose_failed_nodes() {
    require_it!();
    let store = fresh_store().await;
    let tmp = tempfile::tempdir().unwrap();
    store
        .upsert_job(&job(
            "broken",
            &[
                ("build", "dbt build", &[]),
                (
                    "audit",
                    "dbt test --select tag:always_fails --vars '{enable_failing_test: true}'",
                    &["build"],
                ),
            ],
        ))
        .await
        .unwrap();
    let schema = api::schema(store.clone());

    let trigger = r#"mutation { triggerRun(job: "broken", idempotencyKey: "gql-1") { created run { id status } } }"#;
    let first = schema
        .execute(trigger)
        .await
        .into_result()
        .unwrap()
        .data
        .into_json()
        .unwrap();
    let again = schema
        .execute(trigger)
        .await
        .into_result()
        .unwrap()
        .data
        .into_json()
        .unwrap();
    assert_eq!(first["triggerRun"]["created"], true);
    assert_eq!(again["triggerRun"]["created"], false);
    assert_eq!(
        first["triggerRun"]["run"]["id"],
        again["triggerRun"]["run"]["id"]
    );
    let run_id: Uuid = first["triggerRun"]["run"]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    let w = dbt_worker(&store, "w1", tmp.path(), Duration::from_secs(30));
    assert_eq!(
        drive(&store, &[&w], run_id, Duration::from_secs(180))
            .await
            .status,
        "failed"
    );

    // the real relay and projector loops, as `orchestrator all` runs them
    let brokers = env("KAFKA_BROKERS");
    let topic = topic().await;
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let relay_task = tokio::spawn(relay::run(
        store.clone(),
        kafka::KafkaPublisher::new(&brokers, &topic).unwrap(),
        Duration::from_millis(100),
        stop_rx.clone(),
    ));
    let consumer = kafka::consumer(&brokers, &format!("g-{}", Uuid::new_v4()), &topic).unwrap();
    let proj_task = tokio::spawn(kafka::run_projector(store.clone(), consumer, stop_rx));
    let t0 = Instant::now();
    while store.run_history(run_id).await.unwrap().map(|h| h.status) != Some("failed".into()) {
        assert!(
            t0.elapsed() < Duration::from_secs(60),
            "projection did not catch up"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    stop_tx.send(true).unwrap();
    relay_task.await.unwrap().unwrap();
    proj_task.await.unwrap().unwrap();

    let q = format!(
        r#"{{ run(id: "{run_id}") {{ status
              history {{ status nodeExecutions nodeFailures }}
              nodeResults(status: "fail") {{ stepName uniqueId failures message }}
              steps {{ name status attemptHistory {{ status exitCode }} }} }}
            runs(job: "broken") {{ id }} }}"#
    );
    let v = schema
        .execute(q)
        .await
        .into_result()
        .unwrap()
        .data
        .into_json()
        .unwrap();
    let run = &v["run"];
    assert_eq!(run["status"], "failed");
    assert_eq!(run["history"]["status"], "failed");
    assert_eq!(run["history"]["nodeFailures"], 1);
    assert_eq!(
        run["history"]["nodeExecutions"], 14,
        "13 build nodes + 1 audit test"
    );
    let failed = run["nodeResults"].as_array().unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["uniqueId"], "test.jaffle.always_fails");
    assert_eq!(failed[0]["stepName"], "audit");
    assert_eq!(failed[0]["failures"], 5);
    let audit = run["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "audit")
        .unwrap();
    assert_eq!(audit["attemptHistory"][0]["status"], "node_failure");
    assert_eq!(audit["attemptHistory"][0]["exitCode"], 1);
    assert_eq!(v["runs"].as_array().unwrap().len(), 1);

    let cancel = format!(r#"mutation {{ cancelRun(id: "{run_id}") {{ status }} }}"#);
    let v = schema
        .execute(cancel)
        .await
        .into_result()
        .unwrap()
        .data
        .into_json()
        .unwrap();
    assert_eq!(
        v["cancelRun"]["status"], "failed",
        "cancelling a finished run changes nothing"
    );
}
