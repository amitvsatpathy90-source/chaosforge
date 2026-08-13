-- Database: chaosforge_exec | Execution Service
-- V6 — scenario_run (run header) and scenario_run_log (per-step idempotency, ADR-0523).

-- Run header: one row per (scenario_id, replay_version). Phase 1 inserts IN_PROGRESS; Phase 3
-- finalizes. A background sweep marks runs INCOMPLETE if no terminal state after
-- max.poll.interval.ms + grace (Phase-2 crash visibility).
CREATE TABLE scenario_run (
    scenario_id     UUID   NOT NULL,
    replay_version  BIGINT NOT NULL,
    tenant_id       UUID   NOT NULL,
    status          TEXT   NOT NULL DEFAULT 'IN_PROGRESS'
                       CHECK (status IN ('IN_PROGRESS', 'COMPLETED', 'FAILED', 'TIMED_OUT', 'INCOMPLETE')),
    outcome         TEXT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    PRIMARY KEY (scenario_id, replay_version)
);

-- Sweep query support: open runs only.
CREATE INDEX idx_scenario_run_open ON scenario_run (started_at)
    WHERE status = 'IN_PROGRESS';

-- Per-step idempotency primitive (ADR-0523): INSERT ... ON CONFLICT DO NOTHING.
-- PK (scenario_id, replay_version, step_id) prevents duplicate step execution on partial re-run.
CREATE TABLE scenario_run_log (
    scenario_id      UUID   NOT NULL,
    replay_version   BIGINT NOT NULL,
    step_id          TEXT   NOT NULL,
    idempotency_key  TEXT   NOT NULL,        -- scenario_id:replay_version:step_id (mirrors outbound header)
    status           TEXT   NOT NULL
                        CHECK (status IN ('COMPLETED', 'FAILED', 'TIMED_OUT', 'SKIPPED')),
    detail           JSONB,
    recorded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (scenario_id, replay_version, step_id),
    FOREIGN KEY (scenario_id, replay_version) REFERENCES scenario_run (scenario_id, replay_version)
);
