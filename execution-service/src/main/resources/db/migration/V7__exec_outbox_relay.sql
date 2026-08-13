-- Database: chaosforge_exec | Execution Service
-- V7 — bring the result-event outbox to relay parity (Defect B). ADR-0523 Phase 3 writes the result
-- event into `outbox`, but there was no relay shipping it to Kafka. The relay (ExecOutboxPoller)
-- mirrors the CP outbox relay (ADR-0528): claim a due batch FOR UPDATE SKIP LOCKED, publish
-- lock-free, flip SENT only post-broker-ack, back off on failure, quarantine a poison row as DEAD.
--
-- This adds the visibility-timeout column the relay schedules on, and re-homes the status domain
-- from PENDING/SENT/FAILED to PENDING/SENT/DEAD (a transient publish failure folds back to PENDING;
-- DEAD is the terminal poison state — alerted, never the consumer DLQ).

-- Add the visibility-timeout column WITHOUT a volatile default first: a NOT NULL DEFAULT now()
-- would force a full table rewrite under ACCESS EXCLUSIVE (now() is volatile). Add nullable
-- (metadata-only in PG11+), backfill from created_at so existing PENDING rows are immediately due,
-- then attach the default + NOT NULL. No table rewrite, no long live-traffic lock.
ALTER TABLE outbox ADD COLUMN next_attempt_at TIMESTAMPTZ;
UPDATE outbox SET next_attempt_at = created_at WHERE next_attempt_at IS NULL;
ALTER TABLE outbox ALTER COLUMN next_attempt_at SET DEFAULT now();
ALTER TABLE outbox ALTER COLUMN next_attempt_at SET NOT NULL;

-- Fold any transient FAILED rows back to PENDING so the relay reschedules them, then swap the domain.
UPDATE outbox SET status = 'PENDING' WHERE status = 'FAILED';
ALTER TABLE outbox DROP CONSTRAINT IF EXISTS outbox_status_check;
ALTER TABLE outbox ADD CONSTRAINT outbox_status_check CHECK (status IN ('PENDING', 'SENT', 'DEAD'));

-- The poller claims due rows by next_attempt_at; this partial index is the one it uses exclusively.
DROP INDEX IF EXISTS idx_exec_outbox_unsent;
CREATE INDEX idx_exec_outbox_due ON outbox (next_attempt_at) WHERE status = 'PENDING';
