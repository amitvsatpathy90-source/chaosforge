-- Database: chaosforge_cp | Control Plane
-- V12 — run_projection (Batch 4). A CP-side read cache of Exec's terminal ScenarioRunResult events
-- (chaosforge.scenario.results.v1), so the get_run_status MCP tool can answer without CP calling
-- Exec's /internal API — that path is peer-asserted (ADR-0532), the wrong trust model for an
-- MCP-JWT-bound tenant. Exec's scenario_run remains the source of truth; this is a cache, not it.
--
-- One row per (scenario_id, replay_version), matching ScenarioRunResult and Exec's scenario_run PK.
-- No row = the run has never reached a terminal state (still IN_PROGRESS, or the command never
-- arrived) — get_run_status distinguishes that from a stale/failed lookup by row absence.

CREATE TABLE run_projection
(
    scenario_id    UUID        NOT NULL,
    replay_version BIGINT      NOT NULL,
    tenant_id      UUID        NOT NULL,

    -- Mirrors ScenarioRunResult.outcome. Keep as TEXT rather than a DB enum so
    -- evolving Avro outcome values do not require a database enum migration.
    outcome        TEXT        NOT NULL,

    finished_at    TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(), -- last upsert time, for staleness visibility
    PRIMARY KEY (scenario_id, replay_version)
);

CREATE INDEX idx_run_projection_tenant ON run_projection (tenant_id, scenario_id);
