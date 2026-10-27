-- Control-plane state (written by the scheduler and workers) -------------------

CREATE TABLE jobs (
    name               TEXT PRIMARY KEY,
    spec               JSONB NOT NULL,          -- orch_core::JobSpec, validated before upsert
    cron               TEXT,
    max_concurrency    INT  NOT NULL CHECK (max_concurrency >= 1),
    last_scheduled_at  TIMESTAMPTZ,             -- cron watermark
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE runs (
    id                UUID PRIMARY KEY,
    job_name          TEXT NOT NULL REFERENCES jobs (name),
    spec              JSONB NOT NULL,          -- job spec snapshot: a run keeps the config it was triggered with
    trigger           TEXT NOT NULL CHECK (trigger IN ('cron', 'manual')),
    -- cron:<job>:<fire time> for scheduled runs, caller-supplied for manual ones;
    -- the unique constraint is what makes triggering idempotent
    idempotency_key   TEXT NOT NULL UNIQUE,
    status            TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    cancel_requested  BOOLEAN NOT NULL DEFAULT false,
    scheduled_for     TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at        TIMESTAMPTZ,
    finished_at       TIMESTAMPTZ
);
CREATE INDEX runs_job_status ON runs (job_name, status, created_at);

CREATE TABLE steps (
    id                UUID PRIMARY KEY,
    run_id            UUID NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    name              TEXT NOT NULL,
    command           TEXT NOT NULL,
    depends_on        TEXT[] NOT NULL DEFAULT '{}',
    status            TEXT NOT NULL CHECK (status IN ('pending', 'queued', 'running', 'succeeded', 'failed', 'skipped', 'cancelled')),
    attempts          INT NOT NULL DEFAULT 0,
    next_attempt_at   TIMESTAMPTZ,
    -- lease: owner + expiry + epoch (fencing token, bumped on every claim)
    lease_owner       TEXT,
    lease_expires_at  TIMESTAMPTZ,
    lease_epoch       BIGINT NOT NULL DEFAULT 0,
    UNIQUE (run_id, name)
);
CREATE INDEX steps_claimable ON steps (next_attempt_at) WHERE status = 'queued';
CREATE INDEX steps_leased ON steps (lease_expires_at) WHERE status = 'running';

CREATE TABLE attempts (
    id            UUID PRIMARY KEY,
    step_id       UUID NOT NULL REFERENCES steps (id) ON DELETE CASCADE,
    attempt_no    INT NOT NULL,
    worker        TEXT NOT NULL,
    lease_epoch   BIGINT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'node_failure', 'infra_error', 'timed_out', 'lost', 'cancelled')),
    exit_code     INT,
    error         TEXT,
    log_tail      TEXT,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    UNIQUE (step_id, attempt_no)
);

-- Transactional outbox: events are written in the same transaction as the state
-- change they describe, then relayed to Kafka. Publishing is at-least-once.
CREATE TABLE outbox (
    id            BIGSERIAL PRIMARY KEY,
    event_id      UUID NOT NULL UNIQUE,
    run_id        UUID NOT NULL,
    payload       JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

-- Results projection (written only by the Kafka consumer) ----------------------

CREATE TABLE processed_events (
    event_id      UUID PRIMARY KEY,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE run_history (
    run_id           UUID PRIMARY KEY,
    job_name         TEXT NOT NULL,
    status           TEXT NOT NULL,
    queued_at        TIMESTAMPTZ,
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ,
    node_executions  BIGINT NOT NULL DEFAULT 0,
    node_failures    BIGINT NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX run_history_job ON run_history (job_name, queued_at DESC);

CREATE TABLE node_results (
    run_id            UUID NOT NULL,
    step_name         TEXT NOT NULL,
    attempt_no        INT NOT NULL,
    unique_id         TEXT NOT NULL,
    resource_type     TEXT NOT NULL,
    status            TEXT NOT NULL,
    execution_time_s  DOUBLE PRECISION NOT NULL,
    started_at        TIMESTAMPTZ,
    completed_at      TIMESTAMPTZ,
    message           TEXT,
    failures          BIGINT,
    PRIMARY KEY (run_id, step_name, attempt_no, unique_id)
);
