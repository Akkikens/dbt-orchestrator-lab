# dbt-orchestrator-lab

A small control plane for dbt jobs, in Rust: a **scheduler** (cron + manual triggers, per-job concurrency, DAGs of dbt commands), an **execution harness** (workers that run `dbt` as a subprocess under a lease, with timeouts, retries and cancellation), and **results processing** (lifecycle events through Kafka into an idempotent results projection), behind a **GraphQL API**. Built with Claude Code as a learning/portfolio project, to work through the parts of job orchestration that are easy to hand-wave: leases that survive crashed workers, retries that don't double-record results, and an event pipeline that is correct under redelivery. It is not a production system and has no users.

The warehouse is **DuckDB via dbt-duckdb**, not Snowflake: I don't have a Snowflake account. The orchestrator only sees dbt's CLI, exit codes and `run_results.json`, so the adapter matters little here, but nothing in this repo has run against Snowflake.

| Concern | What's here | Where |
|---|---|---|
| Scheduling | 5/6-field cron (UTC) with a per-job watermark; missed ticks coalesce into one run; a scheduled run's idempotency key is `cron:<job>:<fire time>`, so two scheduler replicas (or a retried tick) insert it once. Manual triggers take a caller idempotency key. Per-job `max_concurrency`, admitted oldest-first under the job row lock | `orch-core/src/schedule.rs`, `store.rs` (`schedule_cron`, `admit_runs`) |
| Jobs as DAGs | A job is a list of steps (`dbt build --select ...`) with `depends_on`; validated for unknown deps and cycles; skips cascade downstream of a failure. Step commands are parsed to argv (no shell), limited to `build/run/test/seed/snapshot`, and may not set path flags | `orch-core/src/job.rs`, `planner.rs`, `command.rs` |
| Durable state | Postgres via sqlx with migrations: `jobs`, `runs` (with a snapshot of the job spec), `steps`, `attempts`, `outbox`. Every transition commits together with the events that announce it | `migrations/`, `orchestrator/src/store.rs` |
| Leases + fencing | Workers claim steps with `FOR UPDATE SKIP LOCKED`, heartbeat every TTL/3 (DB clock), and lose the lease if they miss it. The scheduler reaps expired leases (attempt marked `lost`, retry policy applies). Each claim bumps `lease_epoch`; heartbeats and completions must present it, so a zombie worker's late result is rejected | `orch-core/src/lease.rs`, `store.rs` (`claim_step`, `heartbeat`, `complete_attempt`, `reap_expired_leases`) |
| Retries | Exponential backoff with equal jitter (deterministic per step), `max_attempts`. Infra failures (crash, timeout, lost lease) always retry; dbt node failures (failing test, model error) only if the job opts in | `orch-core/src/retry.rs` |
| Execution harness | `dbt` subprocess with a per-attempt `--target-path`, its own process group (timeout/cancel kills dbt and anything it spawned), bounded log tail, exit-code classification, `run_results.json` parsed into per-node status/timing/failures | `orchestrator/src/executor.rs`, `orch-core/src/run_results.rs` |
| Cancellation | `cancelRun` cancels queued work at once; a running worker sees the flag on its next heartbeat, kills dbt and records `cancelled` | `store.rs` (`request_cancel`), `worker.rs` |
| Results processing | Transactional outbox, relayed to Kafka (rdkafka, idempotent producer, keyed by run id). The projector dedupes by event id in the same transaction as the projection writes and commits offsets only afterwards | `relay.rs`, `kafka.rs`, `projector.rs`, `orch-core/src/projection.rs` |
| API | GraphQL (async-graphql + axum): `triggerRun`, `cancelRun`, `jobs`, `runs`, `run { steps { attemptHistory } history nodeResults(status:) }`; GraphiQL at `GET /graphql` | `orchestrator/src/api.rs` |
| Observability | `tracing` JSON logs with a span per attempt (run, step, attempt, epoch, worker); OTLP span export behind the `otel` cargo feature | `telemetry.rs` |

## Architecture

```
  GraphQL client                                  scheduler tick (every 1s)
  triggerRun / cancelRun / queries                cron -> runs, admit (concurrency),
        |                                         reap expired leases, re-plan DAGs
        v                                                 |
  +-------------------------------------------------------v-------+
  | Postgres   jobs  runs  steps  attempts  outbox                 |
  |            run_history  node_results  processed_events         |
  +----^---------------------------^-------------------------------+
       | claim (SKIP LOCKED),      | outbox rows (same tx as state)
       | heartbeat, complete       v
       | (fenced by lease_epoch)  relay --> Kafka topic dbt.run.events (key = run_id)
       |                                         |
  worker --> dbt <command> --target-path <attempt dir>      v
         <-- exit code + target/run_results.json   projector: dedupe by event_id,
                     |                             status only moves forward,
                  DuckDB (dbt-duckdb)              commit offset after the DB commit
```

Two crates: `orch-core` holds every rule (cron, DAG planning, retry, the lease protocol, `run_results` parsing, event ids, projection merge) with no I/O, so each is unit-tested in isolation; `orchestrator` applies those rules inside SQL transactions and runs the processes. `orchestrator all` runs everything in one process; `scheduler`, `worker`, `relay`, `projector` and `api` run separately.

## Try it

```bash
cargo test                       # unit tests; the integration suite skips itself without ORCH_IT=1

docker compose up -d --build --wait     # Postgres + Redpanda + orchestrator
./scripts/smoke.sh                      # trigger jaffle_nightly over GraphQL, wait for the projection
open http://localhost:8080/graphql      # GraphiQL
```

```graphql
mutation { triggerRun(job: "jaffle_broken", idempotencyKey: "demo-1") { created run { id } } }

{ run(id: "...") { status
    steps { name status attempts attemptHistory { status exitCode error } }
    history { status nodeExecutions nodeFailures }
    nodeResults(status: "fail") { uniqueId failures message } } }
```

Without Docker: `orchestrator migrate`, `load-jobs --file jobs.toml`, then `scheduler`, `worker --dbt-bin $(which dbt)` and `api`, pointed at any Postgres with `DATABASE_URL` (all flags also read env vars; see `orchestrator --help`). Jobs live in `jobs.toml`; the sample dbt project is `examples/jaffle` (2 seeds, 3 models, 9 data tests, plus a singular test that only fails when a job enables it with `--vars`).

Integration tests need Postgres, Kafka and dbt-duckdb:

```bash
ORCH_IT=1 DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
KAFKA_BROKERS=localhost:9092 DBT_BIN=$(which dbt) cargo test -p orchestrator --test integration
```

## Design decisions

**Exactly-once effect, not exactly-once delivery.** Nothing here pretends a message or a dbt invocation happens exactly once. A dbt step can run twice (a worker loses its lease mid-run and another one retries it); an event can be published twice (the relay dies after Kafka acks, before it marks the outbox row); a Kafka message can be consumed twice (the consumer dies before committing its offset). What is made exactly-once is the *recorded effect*:

- **Run creation** is keyed by an idempotency key with a unique constraint (cron fire time, or the caller's key for manual triggers).
- **Step results** are accepted only from the holder of the current lease epoch. Every claim bumps the epoch, and completion and reaping re-check it under the run's row lock, so a zombie worker that wakes up after being reclaimed gets `Fenced` and writes nothing. This is the fencing-token pattern; the lease alone would not be enough, because a paused process cannot tell that its lease expired.
- **Events** have deterministic ids (UUIDv5 of run id + what happened, e.g. `node/<step>/<attempt>/<unique_id>`), so a re-recorded transition or a republished outbox row has the same id. All terminal kinds share one id, so a run has at most one terminal event.
- **The projection** inserts the event id into `processed_events` and applies the event in one transaction; a duplicate is a no-op. Counters (`node_executions`) are why this matters: replays would otherwise double them. The run status only moves forward (`queued < running < terminal`, terminal sticks), so a late `started` cannot reopen a finished run.

**Transactional outbox instead of publishing from the worker.** Writing to Postgres and then to Kafka is a dual write: a crash in between loses the event or announces a change that never committed. Events are inserted in the transaction that makes the change and a relay publishes them in id order, keyed by run id so a run's events stay ordered on one partition.

**DB clock for leases.** Lease expiry and heartbeats use Postgres `now()`, so clock skew between worker hosts cannot make a live lease look expired.

**Lock order.** Every transaction that touches a step and then re-plans its run locks the run row first, then step rows, so workers finishing sibling steps serialize instead of deadlocking.

**Failure classes drive retries.** dbt's exit code 1 with a `run_results.json` means a node failed or a test returned rows; retrying the same SQL on the same data usually fails again, so that is opt-in per job. Exit 2, no artifact, a timeout or a lost lease is infrastructure, and is retried with backoff.

## How the guarantees are tested

- **Unit (53, no services):** cron coalescing, first-sight and clock-going-backwards behaviour; DAG validation (cycles, unknown deps, bad commands); planner skip cascades, cancel semantics, admission math; backoff bounds and overflow, retry classification; the lease protocol (crashed worker reclaimed under a new epoch, zombie heartbeat and completion fenced, missed heartbeat loses the lease); `run_results.json` parsing from three real dbt-duckdb 1.12 fixtures (success, failing test, model error with skipped downstream nodes); event id determinism; projection dedupe under triple redelivery in reverse order; executor process control with fake `dbt` scripts (timeout, cancel, lease loss, grandchildren killed via the process group, stale artifacts ignored).
- **Integration (9, real Postgres + Redpanda + dbt-duckdb, in CI):** a 3-step DAG runs end to end with real dbt (13 node results); a failing dbt test is retried, then fails the run and skips its dependent; a "crashed" worker's lease expires, is reaped and re-run under epoch 2, and the zombie's late result is fenced with no events emitted; cancel kills a running process and cancels queued runs; timeouts retry with backoff; idempotent manual triggers and per-job concurrency; cron fires once per window even with a second scheduler holding a stale watermark; every outbox event is published **twice** through Kafka and the projection applies exactly half and keeps correct counts; the real relay and projector loops feed a failed run's node results to the GraphQL API.
- With `ORCH_IT=1`, a missing service is a panic rather than a skip, and CI fails the build if the skip message appears. A separate CI job builds the Docker image, starts the compose stack and runs `scripts/smoke.sh` against it.

## Limits, stated plainly

- **Portfolio project, not production.** No auth on the API, no multi-tenancy, no secrets handling for warehouse credentials, no rate limits, and no load testing. It runs on Linux and macOS only (process groups).
- **DuckDB, not Snowflake.** See above. Concurrency against one DuckDB file is limited (single writer), which is one reason jobs default to `max_concurrency = 1`.
- **Single region, single Postgres.** Postgres is the coordination point; there is no sharding of the scheduler, no partitioned tables and no retention for `outbox`, `processed_events` or attempt logs. One relay instance is assumed for event ordering (`SKIP LOCKED` makes a second one safe but can reorder across instances).
- **Kafka edge cases.** Undecodable messages are logged and skipped, not dead-lettered. The consumer blocks its partition while the projection DB is down, by design. Kafka is exercised against Redpanda in a single-broker dev container only.
- **dbt integration is CLI-level.** No artifact upload, no `--defer`/state comparison, no partial retries of only the failed nodes (`dbt retry`), and logs are a bounded tail in Postgres rather than a log store.
- **OpenTelemetry export** compiles and is linted in CI (`--all-features`) but no collector is exercised in tests; Datadog would be reached via an Agent's OTLP intake, which I have not tried.
