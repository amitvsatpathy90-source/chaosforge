-- Database: chaosforge_cp | Control Plane
-- V1 — tenants. Every tenant-scoped table FKs back here (ADR-0509 three-layer isolation).

CREATE TABLE tenants (
    tenant_id           UUID PRIMARY KEY,
    name                TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'ACTIVE'
                            CHECK (status IN ('ACTIVE', 'SUSPENDED')),
    rate_limit_per_min  INTEGER NOT NULL DEFAULT 600 CHECK (rate_limit_per_min > 0),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Reusable trigger to maintain updated_at on mutation (created once here, reused by later migrations).
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_tenants_updated_at
    BEFORE UPDATE ON tenants
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
