package io.chaosforge.controlplane.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.chaosforge.controlplane.dlq.DlqTriageWatermarkDao;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Proves the DLQ triage watermark's monotonic-upsert contract (ADR-0542, module 1) against real
 * Postgres and the real V11 migration: {@code advance()} only ever moves the high-water forward, and
 * the {@code reviewed_by}/{@code reviewed_at} audit columns are re-stamped only on a genuine advance —
 * so the gauge can trust {@code reviewedOffset()} as "how far a human has actually reviewed".
 */
class DlqTriageWatermarkIT extends CpPostgresIT {

    private static final String TOPIC = "chaosforge.scenario.commands.v1.DLQ";

    private final DlqTriageWatermarkDao dao = new DlqTriageWatermarkDao(jdbc);

    @BeforeEach
    void cleanWatermark() {
        jdbc.update("TRUNCATE dlq_triage_watermark");   // not in CpPostgresIT#cleanTables' cascade list
    }

    @Test
    void absentPartition_readsEmpty() {
        assertThat(dao.reviewedOffset(TOPIC, 3)).isEmpty();
    }

    @Test
    void freshAdvance_insertsAndReturnsOffset() {
        long result = dao.advance(TOPIC, 0, 100L, "operator-a");

        assertThat(result).isEqualTo(100L);
        assertThat(dao.reviewedOffset(TOPIC, 0)).contains(100L);
        assertThat(reviewedBy(0)).isEqualTo("operator-a");
    }

    @Test
    void higherAdvance_movesForwardAndReStampsAudit() {
        dao.advance(TOPIC, 0, 100L, "operator-a");

        long result = dao.advance(TOPIC, 0, 200L, "operator-b");

        assertThat(result).isEqualTo(200L);
        assertThat(dao.reviewedOffset(TOPIC, 0)).contains(200L);
        assertThat(reviewedBy(0)).isEqualTo("operator-b");   // re-stamped on a genuine advance
    }

    @Test
    void lowerAdvance_isNoOpAndKeepsAudit() {
        dao.advance(TOPIC, 0, 100L, "operator-a");

        long result = dao.advance(TOPIC, 0, 50L, "operator-b");

        assertThat(result).isEqualTo(100L);                  // high-water unchanged
        assertThat(dao.reviewedOffset(TOPIC, 0)).contains(100L);
        assertThat(reviewedBy(0)).isEqualTo("operator-a");   // audit NOT re-stamped by a stale advance
    }

    @Test
    void equalAdvance_isNoOpAndKeepsAudit() {
        dao.advance(TOPIC, 0, 100L, "operator-a");

        long result = dao.advance(TOPIC, 0, 100L, "operator-b");

        assertThat(result).isEqualTo(100L);
        assertThat(reviewedBy(0)).isEqualTo("operator-a");   // "strictly greater" gate → equal is a no-op
    }

    @Test
    void partitionsAreIndependent() {
        dao.advance(TOPIC, 0, 100L, "operator-a");
        dao.advance(TOPIC, 1, 5L, "operator-a");

        assertThat(dao.reviewedOffset(TOPIC, 0)).contains(100L);
        assertThat(dao.reviewedOffset(TOPIC, 1)).contains(5L);
        assertThat(dao.reviewedOffset(TOPIC, 2)).isEmpty();
    }

    private String reviewedBy(int partition) {
        return jdbc.queryForObject(
                "SELECT reviewed_by FROM dlq_triage_watermark WHERE topic = ? AND partition = ?",
                String.class, TOPIC, partition);
    }
}
