-- Database: chaosforge_exec | Execution Service
-- V9 — allow the terminal ABORTED run status. A scenario is finalized ABORTED when execution is halted
-- on purpose: the operator kill switch (C19) or an automatic steady-state-breach abort (C20). ABORTED
-- is terminal — never retried, never dead-lettered (the halt was intentional, not a fault to triage).
--
-- Without this, FinalizePhase's UPDATE scenario_run SET status='ABORTED' would violate the V6 CHECK
-- and the listener would never ack → endless reprocessing. (The C19 unit tests used a mock RunLogDao,
-- so this DB-level gap was invisible until the run actually finalized against real Postgres.)

ALTER TABLE scenario_run DROP CONSTRAINT scenario_run_status_check;
ALTER TABLE scenario_run ADD CONSTRAINT scenario_run_status_check
    CHECK (status IN ('IN_PROGRESS', 'COMPLETED', 'FAILED', 'TIMED_OUT', 'INCOMPLETE', 'ABORTED'));
