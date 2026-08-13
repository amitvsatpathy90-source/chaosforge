-- Database: chaosforge_exec | Execution Service
-- V4 — message inbox (dedup) and the Execution Service result-event outbox.

-- Inbox dedup: INSERT ... ON CONFLICT DO NOTHING on message_id (ADR-0007). Catches Kafka
-- redelivery of the same outbox row. TTL-pruned at 7 days; duplicates after that are absorbed by
-- step-level idempotency (scenario_run_log PK).
CREATE TABLE inbox (
    message_id    UUID PRIMARY KEY,         -- == CP outbox message_id (UUIDv7)
    tenant_id     UUID NOT NULL,
    scenario_id   UUID NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Off-peak TTL cleanup: DELETE FROM inbox WHERE processed_at < now() - INTERVAL '7 days'.
CREATE INDEX idx_inbox_processed_at ON inbox (processed_at);

-- Execution Service outbox: result events (ScenarioCompleted / ScenarioTimedOut). Same pattern as CP.
CREATE TABLE outbox (
    message_id      UUID PRIMARY KEY,                   -- UUIDv7
    aggregate_type  TEXT NOT NULL DEFAULT 'scenario_run',
    aggregate_id    UUID NOT NULL,                      -- scenario_id
    tenant_id       UUID NOT NULL,
    topic           TEXT NOT NULL,
    partition_key   TEXT NOT NULL,                      -- = scenario_id (ADR-0526)
    replay_version  BIGINT NOT NULL,
    payload         BYTEA NOT NULL,                     -- Avro binary
    headers         JSONB NOT NULL DEFAULT '{}'::jsonb,
    status          TEXT NOT NULL DEFAULT 'PENDING'
                       CHECK (status IN ('PENDING', 'SENT', 'FAILED')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at         TIMESTAMPTZ
);

CREATE INDEX idx_exec_outbox_unsent ON outbox (message_id)
    WHERE status IN ('PENDING', 'FAILED');
