-- Database: chaosforge_cp | Control Plane
-- V11 — DLQ human-triage watermark (ADR-0542). Backs the standing-DLQ-depth signal that arch-audit F5
-- found missing: `chaosforge.dlq.routed` is a MONOTONIC routing counter, and `.DLQ` consumer-group lag
-- reaches 0 with poison still present (the retry consumer acks hard poison it never republishes,
-- ADR-0529), so neither can answer "how many records await human triage right now". Depth needs a
-- watermark — the exclusive high-water offset a human has reviewed, per (topic, partition) — which the
-- depth gauge subtracts from the Kafka end offset. This table IS that watermark store.
--
-- Advisory triage metadata ONLY: reviewed_offset is NOT a Kafka offset (the retry consumer's group
-- offset is untouched) and this is NOT business/tenant state. DLQ records span tenants, so the table
-- carries no tenant_id and its write path is OPERATOR-gated (ADR-0536). One row per DLQ partition;
-- reviewed_offset advances monotonically (GREATEST) — see DlqTriageWatermarkDao.

CREATE TABLE dlq_triage_watermark (
    topic            TEXT        NOT NULL,
    partition        INT         NOT NULL CHECK (partition >= 0),
    reviewed_offset  BIGINT      NOT NULL CHECK (reviewed_offset >= 0),  -- exclusive high-water: all < reviewed
    reviewed_by      TEXT        NOT NULL,                               -- audit: OPERATOR subject at last advance
    reviewed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition)
);
