-- Database: chaosforge_cp | Control Plane
-- V3 — scenario_replay_state (CAS fencing-token source, ADR-0522) and the CP outbox.

-- One row per scenario; replay_version is the monotonic fencing token and the CAS target (ADR-0522).
CREATE TABLE scenario_replay_state (
    scenario_id      UUID PRIMARY KEY REFERENCES scenarios (scenario_id) ON DELETE CASCADE,
    replay_version   BIGINT NOT NULL DEFAULT 0,
    last_started_at  TIMESTAMPTZ
);

-- Seed the replay-state row at scenario creation (ADR-0522 impl note: row created with
-- replay_version = 0). Guarantees the CAS UPDATE always matches a row, so a 0-row result means
-- contention / stale If-Match — never a missing row.
CREATE OR REPLACE FUNCTION seed_replay_state() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO scenario_replay_state (scenario_id, replay_version)
    VALUES (NEW.scenario_id, 0)
    ON CONFLICT (scenario_id) DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_scenarios_seed_replay_state
    AFTER INSERT ON scenarios
    FOR EACH ROW EXECUTE FUNCTION seed_replay_state();

-- Transactional outbox. Avro-encoded payload at insert (ADR-0525, schema-at-write). UUIDv7 message_id.
-- Status flips to SENT only inside the KafkaTemplate send callback (post-broker-ack).
CREATE TABLE outbox (
    message_id        UUID PRIMARY KEY,                  -- UUIDv7, time-ordered
    aggregate_type    TEXT NOT NULL DEFAULT 'scenario',
    aggregate_id      UUID NOT NULL,                     -- scenario_id
    tenant_id         UUID NOT NULL,
    topic             TEXT NOT NULL,
    partition_key     TEXT NOT NULL,                     -- = scenario_id (ADR-0526; NOT tenant_id:scenario_id)
    replay_version    BIGINT NOT NULL,                   -- fencing token; emitted as x-replay-version
    rule_set_id       UUID NOT NULL,                     -- pinned; emitted as x-rule-set-id
    rule_set_version  INTEGER NOT NULL,                  -- pinned; emitted as x-rule-set-version
    payload           BYTEA NOT NULL,                    -- Avro binary
    tenant_signature  BYTEA,                             -- HMAC over tenant_id (ADR-0524 signed provenance)
    headers           JSONB NOT NULL DEFAULT '{}'::jsonb,
    status            TEXT NOT NULL DEFAULT 'PENDING'
                         CHECK (status IN ('PENDING', 'SENT', 'FAILED')),
    attempts          INTEGER NOT NULL DEFAULT 0,
    last_error        TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at           TIMESTAMPTZ
);

-- Poller: SELECT ... WHERE status IN ('PENDING','FAILED') ORDER BY message_id FOR UPDATE SKIP LOCKED.
-- Partial index keeps the hot path small; message_id (UUIDv7) drains in time order.
CREATE INDEX idx_outbox_unsent ON outbox (message_id)
    WHERE status IN ('PENDING', 'FAILED');
