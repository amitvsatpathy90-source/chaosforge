package io.chaosforge.controlplane.janitor;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Index-backed, chunked garbage collection of expired {@code replay_idempotency} rows (ADR-0528 §TTL
 * janitor). Each call deletes at most {@code chunkSize} rows in a single short, autocommitting
 * statement so transactions stay milliseconds-long — no long-held locks, no WAL blow-out, no lock
 * escalation.
 *
 * <p><b>Index optimality:</b> the cutoff predicate + {@code ORDER BY created_at LIMIT} is served by
 * {@code idx_idem_gc (created_at)} — a bounded index range scan of exactly the oldest {@code chunkSize}
 * expired rows, never a full table scan. Time cost per chunk is O(chunkSize · log N), not O(N).
 *
 * <p><b>Concurrency:</b> the inner {@code SELECT … FOR UPDATE SKIP LOCKED} lets multiple Control Plane
 * instances (or an overlapping run) drain disjoint batches without blocking or deadlocking each other —
 * a row another janitor is already deleting is skipped, not waited on.
 *
 * <p>The cutoff is computed from the database clock ({@code now()}), not the application clock, so
 * retention is immune to app/DB clock skew. Rows are deleted by age regardless of status: a
 * {@code COMPLETED} key past retention is no longer needed for replay, and an {@code IN_PROGRESS} key
 * older than retention is an abandoned claim from a crashed request — reclaiming it is correct.
 */
@Component
public class ReplayIdempotencyGcDao {

    private static final String DELETE_EXPIRED_CHUNK = """
            DELETE FROM replay_idempotency
             WHERE ctid IN (
                   SELECT ctid
                     FROM replay_idempotency
                    WHERE created_at < now() - make_interval(hours => ?)
                    ORDER BY created_at
                    LIMIT ?
                    FOR UPDATE SKIP LOCKED)""";

    private final JdbcTemplate jdbc;

    public ReplayIdempotencyGcDao(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Deletes up to {@code chunkSize} rows older than {@code retentionHours} in one short transaction.
     *
     * @return the number of rows deleted (0 ≤ n ≤ {@code chunkSize}); a value below {@code chunkSize}
     *         means the expired set is drained
     */
    public int deleteExpiredChunk(int retentionHours, int chunkSize) {
        return jdbc.update(DELETE_EXPIRED_CHUNK, retentionHours, chunkSize);
    }
}
