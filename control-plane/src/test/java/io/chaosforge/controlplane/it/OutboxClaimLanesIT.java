package io.chaosforge.controlplane.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.chaosforge.controlplane.outbox.OutboxRecord;
import io.chaosforge.controlplane.outbox.OutboxRelayDao;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The G1 reconciliation, proven against real partitioned Postgres (V8/V9 range partitioning, V10
 * claim attribution):
 *
 * <ul>
 *   <li><b>Plan shape (G1/A-1):</b> the hot-lane claim must touch the partitioned {@code outbox}
 *       through index paths only — no Seq Scan of live partitions (the outer self-join carries the
 *       full {@code (msg_ts, message_id)} PK) and no Sort node (ORDER BY rides
 *       {@code idx_outbox_due} in index order). The legacy {@code message_id IN (subquery)} shape is
 *       kept below as a <b>negative control</b>: the same detector must flag it, or these assertions
 *       are vacuous (the repo's VirtualThreadJdbcPinningIT discipline).</li>
 *   <li><b>Lane complementarity (B-2):</b> a row older than the hot-lane window is invisible to the
 *       hot claim but MUST be claimable by the straggler lane — no PENDING row is architecturally
 *       unreachable before its partition drops.</li>
 *   <li><b>Claim attribution (B-4):</b> a re-claim of another instance's leased-but-unfinalized row
 *       exposes the prior claimant.</li>
 * </ul>
 */
class OutboxClaimLanesIT extends CpPostgresIT {

    /**
     * The pre-V10 claim shape (verbatim), kept as the negative control. Its two defects: the outer
     * UPDATE matches on {@code message_id} alone (cannot use the {@code (msg_ts, message_id)} PK, no
     * other index exists → Seq Scan of every live partition), and {@code ORDER BY message_id} cannot
     * ride {@code idx_outbox_due} → a Sort of the entire due set on every 1s tick.
     */
    private static final String LEGACY_CLAIM_SQL = """
            UPDATE outbox o
               SET attempts = o.attempts + 1,
                   next_attempt_at = now() + make_interval(secs => ?)
             WHERE o.message_id IN (
                   SELECT message_id FROM outbox
                    WHERE status = 'PENDING' AND next_attempt_at <= now()
                    ORDER BY message_id
                    LIMIT ?
                    FOR UPDATE SKIP LOCKED)
            RETURNING o.message_id""";

    @Test
    void hotClaimPlan_indexPathsOnly_noSeqScan_noSortNode() {
        // Volume matters: on a near-empty table the planner CORRECTLY prefers hash-join-over-seq-scan
        // (scanning 3k rows is cheaper than 64 index probes). The defect this guards against only
        // manifests when partitions are large — the post-outage 10x backlog — so seed enough rows
        // that the planner faces the real trade-off, then require it lands on the PK probes.
        seedBulkPending(150_000);
        String populated = mostPopulatedPartition();

        String plan = explain(OutboxRelayDao.CLAIM_SQL, 2, 64, 30, "plan-probe");

        // EMPTY partitions legitimately show cost-0 Seq Scans inside the runtime-pruned Append
        // (scanning zero pages beats an index probe, and pruning skips them at execution anyway) —
        // the invariant is that the partition actually HOLDING the backlog is PK-probed, never scanned.
        assertThat(plan)
                .as("outer self-join carries the PK → the populated partition is index-probed\n%s", plan)
                .contains("Index Scan using " + populated + "_pkey");
        assertThat(plan)
                .as("the populated partition must never be Seq Scanned on the 1s claim path\n%s", plan)
                .doesNotContain("Seq Scan on " + populated);
        assertThat(plan)
                .as("ORDER BY next_attempt_at rides idx_outbox_due in index order — no Sort of the due set\n%s", plan)
                .doesNotContain("->  Sort")
                .doesNotContain("Incremental Sort");
    }

    @Test
    void legacyClaimShape_negativeControl_plannerNeedsSeqScanAndSort() {
        // Proves the detector is sighted: on the same data, the pre-V10 shape MUST exhibit both
        // defects the assertions above forbid. If this test ever passes with a clean plan, the plan
        // assertions in this file are vacuous and must be revisited.
        seedBulkPending(150_000);
        String populated = mostPopulatedPartition();

        String plan = explain(LEGACY_CLAIM_SQL, 30, 64);

        assertThat(plan)
                .as("message_id-only match has no usable index — the populated partition is Seq Scanned\n%s", plan)
                .contains("Seq Scan on " + populated);
        assertThat(plan)
                .as("ORDER BY message_id cannot ride idx_outbox_due — the whole due set is sorted\n%s", plan)
                .contains("->  Sort");
    }

    @Test
    void hotLaneSkipsOutsideWindowRow_stragglerLaneClaimsIt() {
        UUID fresh = insertPendingAgedDays(0);
        UUID straggler = insertPendingAgedDays(4);   // older than the 2-day hot window
        OutboxRelayDao dao = new OutboxRelayDao(jdbc);

        List<OutboxRecord> hot = tx.execute(s -> dao.claimAndLease(64, 30, 2, "hot-lane"));
        assertThat(hot).extracting(OutboxRecord::messageId)
                .as("hot lane sees only rows inside the claim window").containsExactly(fresh);

        List<OutboxRecord> cold = tx.execute(s -> dao.claimAndLeaseStragglers(64, 30, 2, "cold-lane"));
        assertThat(cold).extracting(OutboxRecord::messageId)
                .as("straggler lane reaches the complement — no row is unreachable (B-2)")
                .containsExactly(straggler);

        // The gauge source that alerts on straggler-lane-only rows sees exactly the old row.
        assertThat(dao.pendingOutsideWindowCount(2)).isEqualTo(1L);
    }

    @Test
    void reclaimByAnotherInstance_exposesPriorClaimant() {
        UUID id = insertPendingAgedDays(0);
        OutboxRelayDao dao = new OutboxRelayDao(jdbc);

        List<OutboxRecord> first = tx.execute(s -> dao.claimAndLease(64, 30, 2, "relay-A"));
        assertThat(first).hasSize(1);
        assertThat(first.get(0).priorClaimedBy()).as("first claim has no prior claimant").isNull();

        // relay-A never finalizes (crash / hung publish); the lease elapses.
        jdbc.update("UPDATE outbox SET next_attempt_at = now() WHERE message_id = ?", id);

        List<OutboxRecord> second = tx.execute(s -> dao.claimAndLease(64, 30, 2, "relay-B"));
        assertThat(second).hasSize(1);
        assertThat(second.get(0).priorClaimedBy())
                .as("the duplicate-publish window is attributable (B-4)").isEqualTo("relay-A");
    }

    /** EXPLAIN through the same prepared-statement path production uses; joined into one plan text. */
    private String explain(String sql, Object... args) {
        List<String> lines = jdbc.queryForList("EXPLAIN " + sql, String.class, args);
        return String.join("\n", lines);
    }

    /** The partition holding the seeded backlog, named by the DB itself (immune to midnight flake). */
    private String mostPopulatedPartition() {
        return jdbc.queryForObject(
                "SELECT tableoid::regclass::text FROM outbox GROUP BY 1 ORDER BY count(*) DESC LIMIT 1",
                String.class);
    }

    /** Bulk-seeds due PENDING rows across three live day-partitions, then ANALYZEs for real stats. */
    private void seedBulkPending(int rows) {
        jdbc.update("""
                INSERT INTO outbox (message_id, msg_ts, aggregate_id, tenant_id, topic, partition_key,
                                    replay_version, rule_set_id, rule_set_version, payload)
                SELECT gen_random_uuid(),
                       now() - make_interval(days => CASE WHEN i % 10 = 0 THEN 3
                                                          WHEN i % 10 = 1 THEN 5 ELSE 0 END),
                       gen_random_uuid(), gen_random_uuid(), 'chaosforge.scenario.commands.v1',
                       i::text, 1, gen_random_uuid(), 1, '\\x01'::bytea
                  FROM generate_series(1, ?) AS i""", rows);
        jdbc.execute("ANALYZE outbox");
    }

    private UUID insertPendingAgedDays(int agedDays) {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO outbox (message_id, msg_ts, aggregate_id, tenant_id, topic, "
                        + "partition_key, replay_version, rule_set_id, rule_set_version, payload) "
                        + "VALUES (?, now() - make_interval(days => ?), ?, ?, ?, ?, ?, ?, ?, ?)",
                id, agedDays, UUID.randomUUID(), UUID.randomUUID(), "chaosforge.scenario.commands.v1",
                id.toString(), 1L, UUID.randomUUID(), 1, new byte[] {0x1});
        return id;
    }
}
