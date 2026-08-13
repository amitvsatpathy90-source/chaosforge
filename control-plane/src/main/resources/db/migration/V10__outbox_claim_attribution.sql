-- Database: chaosforge_cp | Control Plane
-- V10 — relay claim attribution (arch-audit G1 reconciliation). Every lease records WHICH relay
-- instance claimed the row and WHEN, so a re-claim of a still-PENDING row by a different instance
-- (a lease takeover — the duplicate-publish window) is an observable counter instead of an inference.
-- Nullable columns on a partitioned table are metadata-only — no table rewrite, no long lock.

ALTER TABLE outbox ADD COLUMN claimed_by TEXT;
ALTER TABLE outbox ADD COLUMN claimed_at TIMESTAMPTZ;
