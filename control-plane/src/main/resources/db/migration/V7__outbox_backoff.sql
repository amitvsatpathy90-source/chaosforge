-- Database: chaosforge_cp | Control Plane
-- V7 — additive backoff support for the outbox relay (expand-only; FULL_TRANSITIVE-safe by analogy).
-- next_attempt_at gates retries: the poller claims rows with next_attempt_at <= now(), and markFailed
-- pushes it forward by min(base * 2^attempts, cap) seconds.
ALTER TABLE outbox ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- Supports the SKIP-LOCKED claim predicate (due + retriable rows only).
CREATE INDEX idx_outbox_due ON outbox (next_attempt_at) WHERE status IN ('PENDING', 'FAILED');
