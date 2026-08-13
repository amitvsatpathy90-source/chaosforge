-- Database: chaosforge_exec | Execution Service
-- V5 — inbox_fence: durable max-seen replay_version per scenario (ADR-0523 Phase 1 fencing).
--
-- Keyed by scenario_id (UUID is globally unique → equivalent to per tenant_id:scenario_id).
-- Hydrated into memory on consumer startup BEFORE the listener activates. Phase 1 does
-- SELECT ... FOR UPDATE on the row (short tx), compares the incoming x-replay-version, and advances
-- max_seen. Fencing precedes inbox dedup — a stale version is structurally invalid, not a duplicate.
-- max_seen defaults to -1 (mirrors the consumer's absent-row convention: getMaxSeen(...).orElse(-1));
-- the first command carries replay_version = 1, so it passes (1 > -1).

CREATE TABLE inbox_fence (
    scenario_id   UUID PRIMARY KEY,
    tenant_id     UUID   NOT NULL,
    max_seen      BIGINT NOT NULL DEFAULT -1,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
