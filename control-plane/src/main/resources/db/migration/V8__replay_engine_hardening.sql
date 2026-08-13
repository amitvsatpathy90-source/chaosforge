-- Database: chaosforge_cp | Control Plane
-- V8 — ADR-0528: ownership-first replay critical section, client idempotency, outbox poison quarantine.
-- Narrows ADR-0522: tenant predicate on the CAS row, a replay_idempotency claim table, and a DEAD
-- terminal status on the outbox so a poison row stops blocking the poller after MAX_ATTEMPTS.

-- ── scenario_replay_state: add tenant_id (CAS defense-in-depth) + non-negative invariant ──────────
-- tenant_id is belt-and-suspenders at the write: ownership is already proven in step 1, but carrying
-- tenant_id in the CAS WHERE prevents any cross-tenant mutation under a future reorder/skip of step 1.
ALTER TABLE scenario_replay_state ADD COLUMN tenant_id UUID;

UPDATE scenario_replay_state st
   SET tenant_id = s.tenant_id
  FROM scenarios s
 WHERE s.scenario_id = st.scenario_id;

ALTER TABLE scenario_replay_state ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE scenario_replay_state
    ADD CONSTRAINT chk_replay_version_nonneg CHECK (replay_version >= 0);

-- Seed trigger now carries tenant_id from the parent scenario (same-tx row creation, ADR-0528 §Schema).
CREATE OR REPLACE FUNCTION seed_replay_state() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO scenario_replay_state (scenario_id, tenant_id, replay_version)
    VALUES (NEW.scenario_id, NEW.tenant_id, 0)
    ON CONFLICT (scenario_id) DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ── scenario_rule_binding: the step-1 ownership probe surface (ADR-0528) ──────────────────────────
-- A tenant-scoped projection of the pinned rule-set tuple. The critical section reads THIS (proving
-- existence + ownership) and never reads replay_version here — so this is not the forbidden
-- read-then-CAS pattern (the CAS predicate still uses the client-supplied expectedVersion).
CREATE VIEW scenario_rule_binding AS
SELECT scenario_id, tenant_id, rule_set_id, rule_set_version
  FROM scenarios;

-- ── replay_idempotency: at-most-once client retry guard (ADR-0528) ────────────────────────────────
-- A replayed POST whose 202 was lost in transit returns the original token instead of re-running CAS.
CREATE TABLE replay_idempotency (
    tenant_id         UUID        NOT NULL,
    idempotency_key   UUID        NOT NULL,
    status            TEXT        NOT NULL CHECK (status IN ('IN_PROGRESS', 'COMPLETED')),
    scenario_id       UUID,
    fencing_token     BIGINT,
    rule_set_id       UUID,
    rule_set_version  INTEGER,
    outbox_message_id UUID,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, idempotency_key)
);

-- TTL janitor support (nightly: DELETE WHERE created_at < now() - INTERVAL '24 hours').
CREATE INDEX idx_idem_gc ON replay_idempotency (created_at);

-- ── outbox: FAILED → DEAD terminal quarantine (ADR-0528 §Outbox Poller) ───────────────────────────
-- New status model: a transient failure keeps the row PENDING (retried via next_attempt_at backoff);
-- only attempts >= MAX_ATTEMPTS flips it to DEAD (terminal, alerted — NOT the consumer DLQ).
UPDATE outbox SET status = 'PENDING' WHERE status = 'FAILED';

ALTER TABLE outbox DROP CONSTRAINT outbox_status_check;
ALTER TABLE outbox
    ADD CONSTRAINT outbox_status_check CHECK (status IN ('PENDING', 'SENT', 'DEAD'));

-- Claimable set is now PENDING-only (DEAD is terminal). Realign the partial indexes.
DROP INDEX idx_outbox_unsent;
DROP INDEX idx_outbox_due;
CREATE INDEX idx_outbox_due ON outbox (next_attempt_at) WHERE status = 'PENDING';
