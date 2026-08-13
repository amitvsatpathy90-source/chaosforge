-- Database: chaosforge_cp | Control Plane
-- V9 — partition the CP `outbox` by day for O(1) partition-drop purge (acceptance gate C28), mirroring
-- the Execution Service V8 change. The CP outbox has no dedup (every message_id is unique per replay),
-- so its partition key `msg_ts` simply defaults to now() (≈ creation time) — no producer-side change to
-- the insert is needed (OutboxRepository.insert is a column-list that omits msg_ts).
--
-- Purge becomes a DROP of an aged day-partition (PartitionMaintenance) instead of
-- `DELETE ... WHERE status='SENT' AND sent_at < cutoff` — O(1), no dead tuples, no autovacuum churn.

-- ---- partition helpers (per-DB; the exec DB defines its own copy) --------------------------------
CREATE OR REPLACE FUNCTION cf_ensure_daily_partition(parent text, day date)
RETURNS void AS $$
DECLARE
    child text := format('%s_p%s', parent, to_char(day, 'YYYYMMDD'));
BEGIN
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                   child, parent, day::timestamptz, (day + 1)::timestamptz);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cf_drop_partitions_before(parent text, cutoff date)
RETURNS integer AS $$
DECLARE
    child   text;
    dropped int := 0;
BEGIN
    FOR child IN
        SELECT c.relname
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        JOIN pg_class p ON p.oid = i.inhparent
        WHERE p.relname = parent AND c.relname ~ ('^' || parent || '_p[0-9]{8}$')
    LOOP
        IF to_date(right(child, 8), 'YYYYMMDD') < cutoff THEN
            EXECUTE format('DROP TABLE %I', child);
            dropped := dropped + 1;
        END IF;
    END LOOP;
    RETURN dropped;
END;
$$ LANGUAGE plpgsql;

-- ---- outbox: recreate partitioned, preserving the post-V8 shape ----------------------------------
DROP TABLE outbox;
CREATE TABLE outbox (
    message_id        UUID NOT NULL,
    msg_ts            TIMESTAMPTZ NOT NULL DEFAULT now(),   -- partition key; no dedup ⇒ creation time is fine
    aggregate_type    TEXT NOT NULL DEFAULT 'scenario',
    aggregate_id      UUID NOT NULL,
    tenant_id         UUID NOT NULL,
    topic             TEXT NOT NULL,
    partition_key     TEXT NOT NULL,
    replay_version    BIGINT NOT NULL,
    rule_set_id       UUID NOT NULL,
    rule_set_version  INTEGER NOT NULL,
    payload           BYTEA NOT NULL,
    tenant_signature  BYTEA,
    headers           JSONB NOT NULL DEFAULT '{}'::jsonb,
    status            TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'SENT', 'DEAD')),
    attempts          INTEGER NOT NULL DEFAULT 0,
    last_error        TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at           TIMESTAMPTZ,
    next_attempt_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (msg_ts, message_id)
) PARTITION BY RANGE (msg_ts);
CREATE TABLE outbox_default PARTITION OF outbox DEFAULT;
CREATE INDEX idx_outbox_due ON outbox (next_attempt_at) WHERE status = 'PENDING';

-- ---- seed a window of daily partitions around migration time -------------------------------------
DO $$
DECLARE d date;
BEGIN
    FOR d IN SELECT generate_series((now() - interval '8 days')::date, (now() + interval '2 days')::date, '1 day')::date
    LOOP
        PERFORM cf_ensure_daily_partition('outbox', d);
    END LOOP;
END $$;
