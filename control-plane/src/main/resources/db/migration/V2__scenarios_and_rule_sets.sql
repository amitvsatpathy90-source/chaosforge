-- Database: chaosforge_cp | Control Plane
-- V2 — rule_sets (append-only, ADR-0503) and scenarios (tenant-scoped, pin a rule-set version).

-- Append-only rule sets: identity is (rule_set_id, version). Never updated, never deleted.
CREATE TABLE rule_sets (
    rule_set_id  UUID    NOT NULL,
    version      INTEGER NOT NULL,
    tenant_id    UUID    NOT NULL REFERENCES tenants (tenant_id),
    name         TEXT    NOT NULL,
    definition   JSONB   NOT NULL,                 -- canonical step / fault definition
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (rule_set_id, version)
);

CREATE INDEX idx_rule_sets_tenant ON rule_sets (tenant_id, rule_set_id);

-- Structural append-only enforcement (ADR-0503): block UPDATE/DELETE at the DB. The repository
-- also rejects a PUT on an existing (rule_set_id, version); this trigger is the backstop the
-- Testcontainers suite asserts ("PUT on existing (rule_set_id, version) -> rejected").
CREATE OR REPLACE FUNCTION reject_rule_set_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'rule_sets are append-only (ADR-0503): % on (%, %) rejected',
        TG_OP, OLD.rule_set_id, OLD.version;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_rule_sets_append_only
    BEFORE UPDATE OR DELETE ON rule_sets
    FOR EACH ROW EXECUTE FUNCTION reject_rule_set_mutation();

-- Scenarios pin a specific (rule_set_id, version). The replay CAS tx reads this pin.
CREATE TABLE scenarios (
    scenario_id       UUID PRIMARY KEY,
    tenant_id         UUID NOT NULL REFERENCES tenants (tenant_id),
    name              TEXT NOT NULL,
    rule_set_id       UUID NOT NULL,
    rule_set_version  INTEGER NOT NULL,
    status            TEXT NOT NULL DEFAULT 'ACTIVE'
                         CHECK (status IN ('ACTIVE', 'ARCHIVED')),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (rule_set_id, rule_set_version) REFERENCES rule_sets (rule_set_id, version)
);

CREATE INDEX idx_scenarios_tenant ON scenarios (tenant_id, scenario_id);

CREATE TRIGGER trg_scenarios_updated_at
    BEFORE UPDATE ON scenarios
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
