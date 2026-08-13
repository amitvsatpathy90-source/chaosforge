-- Database: chaosforge_exec | Execution Service
-- V8 — partition `inbox` and `outbox` by day for O(1) partition-drop purge (acceptance gate C28).
--
-- WHY partition-drop instead of `DELETE ... WHERE processed_at < cutoff`: a bulk DELETE leaves dead
-- tuples that autovacuum must reclaim, and on a hot append-only table it falls behind under load
-- (index bloat, table bloat, vacuum churn). Dropping a whole day-partition is a metadata operation:
-- O(1), no dead tuples, no vacuum. This is the path the outbox-purge comments already promised.
--
-- THE INBOX DEDUP CONSTRAINT (load-bearing): the inbox dedups on message_id, so its partition key must
-- be IDENTICAL across redeliveries of the same message — `processed_at` (a fresh now() each delivery)
-- would scatter redeliveries across partitions and break dedup. Resolution: `msg_ts` is the timestamp
-- EMBEDDED in the UUIDv7 message_id (the app sets it via UuidUtil.getInstant — deterministic from
-- message_id), so the same message_id always maps to the same partition and the composite unique key
-- (msg_ts, message_id) is exactly message_id dedup, while old partitions still drop by age.
-- The outbox has no dedup, so its msg_ts simply defaults to now() (≈ creation time).

-- ---- partition helpers (reused by the Java PartitionMaintenance job) ----------------------------

-- Ensure a daily partition exists (idempotent). The name encodes the day for age-based DROP.
CREATE OR REPLACE FUNCTION cf_ensure_daily_partition(parent text, day date)
RETURNS void AS $$
DECLARE
    child text := format('%s_p%s', parent, to_char(day, 'YYYYMMDD'));
BEGIN
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                   child, parent, day::timestamptz, (day + 1)::timestamptz);
END;
$$ LANGUAGE plpgsql;

-- Drop every daily partition of `parent` whose day is strictly before `cutoff`. Returns the count.
-- The DEFAULT partition (…_default) never matches the _pYYYYMMDD pattern, so it is never dropped.
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

-- ---- inbox: recreate partitioned (lab recreate; a prod migration would expand-contract copy) -----
DROP TABLE inbox;
CREATE TABLE inbox (
    message_id    UUID NOT NULL,
    msg_ts        TIMESTAMPTZ NOT NULL,        -- = UuidUtil.getInstant(message_id); deterministic ⇒ dedup-safe
    tenant_id     UUID NOT NULL,
    scenario_id   UUID NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (msg_ts, message_id)           -- partition key first; (msg_ts ⇐ message_id) ⇒ global dedup
) PARTITION BY RANGE (msg_ts);
CREATE TABLE inbox_default PARTITION OF inbox DEFAULT;   -- safety net: an out-of-window msg_ts still inserts
CREATE INDEX idx_inbox_processed_at ON inbox (processed_at);

-- ---- outbox: recreate partitioned, preserving the post-V7 shape (next_attempt_at, DEAD domain) ----
DROP TABLE outbox;
CREATE TABLE outbox (
    message_id      UUID NOT NULL,
    msg_ts          TIMESTAMPTZ NOT NULL DEFAULT now(),   -- no dedup ⇒ creation time is fine as the partition key
    aggregate_type  TEXT NOT NULL DEFAULT 'scenario_run',
    aggregate_id    UUID NOT NULL,
    tenant_id       UUID NOT NULL,
    topic           TEXT NOT NULL,
    partition_key   TEXT NOT NULL,
    replay_version  BIGINT NOT NULL,
    payload         BYTEA NOT NULL,
    headers         JSONB NOT NULL DEFAULT '{}'::jsonb,
    status          TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'SENT', 'DEAD')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at         TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (msg_ts, message_id)
) PARTITION BY RANGE (msg_ts);
CREATE TABLE outbox_default PARTITION OF outbox DEFAULT;
CREATE INDEX idx_exec_outbox_due ON outbox (next_attempt_at) WHERE status = 'PENDING';

-- ---- seed a window of daily partitions around migration time -------------------------------------
-- So inserts route to droppable daily partitions (not the default) from the first command onward.
DO $$
DECLARE d date;
BEGIN
    FOR d IN SELECT generate_series((now() - interval '8 days')::date, (now() + interval '2 days')::date, '1 day')::date
    LOOP
        PERFORM cf_ensure_daily_partition('inbox', d);
        PERFORM cf_ensure_daily_partition('outbox', d);
    END LOOP;
END $$;
