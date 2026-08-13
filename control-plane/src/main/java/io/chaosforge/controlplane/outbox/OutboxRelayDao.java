package io.chaosforge.controlplane.outbox;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

/**
 * JdbcTemplate access for the outbox relay (ADR-0528): lease-on-claim under SKIP LOCKED, then one
 * batched finalize tx per tick. Every statement predicates on the full (msg_ts, message_id) PK so
 * it prunes to one partition (partitioning-rules §2) — a message_id-only predicate scans all partitions.
 * Two complementary lanes: hot (younger than claim-window-days, ordered by next_attempt_at to ride
 * the index) and straggler (older, oldest-first, slow cadence) so no PENDING row is ever unreachable
 * before its partition drops. Relay ordering is best-effort; fencing + inbox dedup absorb reorder.
 * Crash between claim/finalize leaves the row PENDING for lease retry; claimed_by tracks lease takeovers.
 */
@Component
public class OutboxRelayDao {

    // Hot lane: atomic claim + lease. Public so OutboxClaimLanesIT can EXPLAIN the real statement.
    public static final String CLAIM_SQL = """
            WITH due AS MATERIALIZED (
                SELECT msg_ts, message_id, claimed_by
                  FROM outbox
                 WHERE status = 'PENDING' AND next_attempt_at <= now()
                   AND msg_ts >= now() - make_interval(days => ?)
                 ORDER BY next_attempt_at
                 LIMIT ?
                   FOR UPDATE SKIP LOCKED)
            UPDATE outbox o
               SET attempts = o.attempts + 1,
                   next_attempt_at = now() + make_interval(secs => ?),
                   claimed_by = ?,
                   claimed_at = now()
              FROM due
             WHERE o.msg_ts = due.msg_ts AND o.message_id = due.message_id
            RETURNING o.message_id, o.msg_ts, o.tenant_id, o.topic, o.partition_key, o.replay_version,
                      o.rule_set_id, o.rule_set_version, o.payload, o.attempts,
                      due.claimed_by AS prior_claimed_by""";

    // Straggler lane: strictly older than the window, oldest first. Liveness lane, not throughput.
    static final String STRAGGLER_CLAIM_SQL = """
            WITH due AS MATERIALIZED (
                SELECT msg_ts, message_id, claimed_by
                  FROM outbox
                 WHERE status = 'PENDING' AND next_attempt_at <= now()
                   AND msg_ts < now() - make_interval(days => ?)
                 ORDER BY msg_ts
                 LIMIT ?
                   FOR UPDATE SKIP LOCKED)
            UPDATE outbox o
               SET attempts = o.attempts + 1,
                   next_attempt_at = now() + make_interval(secs => ?),
                   claimed_by = ?,
                   claimed_at = now()
              FROM due
             WHERE o.msg_ts = due.msg_ts AND o.message_id = due.message_id
            RETURNING o.message_id, o.msg_ts, o.tenant_id, o.topic, o.partition_key, o.replay_version,
                      o.rule_set_id, o.rule_set_version, o.payload, o.attempts,
                      due.claimed_by AS prior_claimed_by""";

    private static final RowMapper<OutboxRecord> MAPPER = (rs, n) -> new OutboxRecord(
            rs.getObject("message_id", UUID.class),
            rs.getObject("msg_ts", OffsetDateTime.class),
            rs.getObject("tenant_id", UUID.class),
            rs.getString("topic"),
            rs.getString("partition_key"),
            rs.getLong("replay_version"),
            rs.getObject("rule_set_id", UUID.class),
            rs.getInt("rule_set_version"),
            rs.getBytes("payload"),
            rs.getInt("attempts"),
            rs.getString("prior_claimed_by"));

    private final JdbcTemplate jdbc;

    public OutboxRelayDao(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Hot-lane claim: due rows younger than the window. Run inside its own short transaction. */
    public List<OutboxRecord> claimAndLease(int batchSize, int leaseSeconds, int claimWindowDays,
                                            String instanceId) {
        return jdbc.query(CLAIM_SQL, MAPPER, claimWindowDays, batchSize, leaseSeconds, instanceId);
    }

    /** Straggler-lane claim: due rows OLDER than the window, oldest first. Own short transaction. */
    public List<OutboxRecord> claimAndLeaseStragglers(int batchSize, int leaseSeconds, int claimWindowDays,
                                                      String instanceId) {
        return jdbc.query(STRAGGLER_CLAIM_SQL, MAPPER, claimWindowDays, batchSize, leaseSeconds, instanceId);
    }

    /** SENT after broker ack — one JDBC batch, PK-predicated, inside the poller's finalize tx. */
    public void markSentBatch(List<OutboxRecord> sent) {
        if (sent.isEmpty()) {
            return;
        }
        jdbc.batchUpdate(
                "UPDATE outbox SET status = 'SENT', sent_at = now() WHERE msg_ts = ? AND message_id = ?",
                sent, sent.size(), (ps, r) -> {
                    ps.setObject(1, r.msgTs());
                    ps.setObject(2, r.messageId());
                });
    }

    /** Record error + backoff; DEAD only if recordFatal and at maxAttempts, else backs off forever. */
    public void markFailedOrDeadBatch(List<FailedPublish> failed, int maxAttempts,
                                      double baseSeconds, double capSeconds) {
        if (failed.isEmpty()) {
            return;
        }
        jdbc.batchUpdate("""
                UPDATE outbox
                   SET last_error = ?,
                       status = CASE WHEN ? AND attempts >= ? THEN 'DEAD' ELSE 'PENDING' END,
                       next_attempt_at = now() + make_interval(secs => least(? * power(2, attempts), ?))
                 WHERE msg_ts = ? AND message_id = ?""",
                failed, failed.size(), (ps, f) -> {
                    ps.setString(1, f.error());
                    ps.setBoolean(2, f.recordFatal());
                    ps.setInt(3, maxAttempts);
                    ps.setDouble(4, baseSeconds);
                    ps.setDouble(5, capSeconds);
                    ps.setObject(6, f.row().msgTs());
                    ps.setObject(7, f.row().messageId());
                });
    }

    public long pendingCount() {
        Long count = jdbc.queryForObject("SELECT count(*) FROM outbox WHERE status = 'PENDING'", Long.class);
        return count == null ? 0L : count;
    }

    public long deadCount() {
        Long count = jdbc.queryForObject("SELECT count(*) FROM outbox WHERE status = 'DEAD'", Long.class);
        return count == null ? 0L : count;
    }

    public double oldestPendingAgeSeconds() {
        Double age = jdbc.queryForObject(
                "SELECT COALESCE(EXTRACT(EPOCH FROM now() - MIN(created_at)), 0) FROM outbox WHERE status = 'PENDING'",
                Double.class);
        return age == null ? 0d : age;
    }

    /** Rows only the straggler lane can reach — gauge {@code outbox.pending_outside_claim_window}. */
    public long pendingOutsideWindowCount(int claimWindowDays) {
        Long count = jdbc.queryForObject(
                "SELECT count(*) FROM outbox WHERE status = 'PENDING' AND msg_ts < now() - make_interval(days => ?)",
                Long.class, claimWindowDays);
        return count == null ? 0L : count;
    }

    // Delivered-row purge is by partition-drop (PartitionMaintenance, C28), not a status-scoped DELETE.
}
