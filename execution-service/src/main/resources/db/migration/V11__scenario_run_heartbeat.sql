-- Database: chaosforge_exec | Execution Service
-- V11 — per-attempt heartbeat on scenario_run so the INCOMPLETE sweep measures time-since-last-activity,
-- not time-since-first-start (arch-audit H2).
--
-- THE BUG: `started_at` is stamped once, at the FIRST Phase-1 claim, and never moves (insertRunHeader was
-- ON CONFLICT DO NOTHING). A command that fails Phase 2 (STEP_TIMEOUT / INFRA_TRANSIENT) is republished by
-- the DLQ retry consumer with a FRESH message_id and re-executed — each redelivery gets a fresh 240s
-- aggregate budget, so a run legitimately bounces through the retry lane for many minutes while `started_at`
-- stays frozen at t0. The sweep threshold (360s = max.poll.interval.ms + grace) was chosen to never sweep a
-- run inside ONE poll budget, but across DLQ redeliveries the run spans many poll budgets — so the sweep
-- would fire on a still-active run, emit a premature terminal INCOMPLETE event, and (with the pre-V11
-- unguarded finalize) let a later successful attempt emit a SECOND terminal event + overwrite the status.
--
-- THE FIX: heartbeat. insertRunHeader now ON CONFLICT DO UPDATE SET last_attempt_at = now(), so every
-- Phase-1 claim (i.e. every fresh-message_id redelivery from the retry lane) advances the clock. The sweep
-- predicates on last_attempt_at, so it only fires once a run has been QUIET for the threshold — which is
-- exactly the genuine Phase-2 crash case (same message_id, ack-skipped by inbox dedup, so Phase 1 never
-- re-runs and the heartbeat never advances). An actively-retrying run keeps its heartbeat fresh and is left
-- alone. Paired with the status-guarded finalize (RunLogDao.finalizeRun … AND status='IN_PROGRESS'), a run
-- reaches a terminal event exactly once.

ALTER TABLE scenario_run ADD COLUMN last_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- Backfill existing rows to their start so the sweep behaves identically for runs created before V11
-- (the ADD COLUMN default stamps migration-time now(); an in-flight pre-V11 run must not get a free reprieve).
UPDATE scenario_run SET last_attempt_at = started_at;

-- The sweep now scans by inactivity, not by birth. Same partial-index shape, new leading column.
DROP INDEX idx_scenario_run_open;
CREATE INDEX idx_scenario_run_open ON scenario_run (last_attempt_at) WHERE status = 'IN_PROGRESS';
