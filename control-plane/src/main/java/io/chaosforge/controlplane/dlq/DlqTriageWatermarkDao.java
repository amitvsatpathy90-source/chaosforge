package io.chaosforge.controlplane.dlq;

import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Persistence for the DLQ human-triage watermark (ADR-0542) — the high-water offset a human has
 * reviewed, per (topic, partition). The depth gauge subtracts this from the Kafka end offset
 * (arch-audit F5; neither the routing counter nor consumer lag can answer this).
 * The one stateful corner of DLQ triage — writes only CP Postgres, never Kafka; the advisory LLM
 * path stays read-only. OPERATOR-gated; reviewed_by/reviewed_at are the audit trail.
 */
@Component
public class DlqTriageWatermarkDao {

    // Monotonic upsert: reviewed_offset only moves forward; audit columns re-stamp only on real advance.
    private static final String ADVANCE = """
            INSERT INTO dlq_triage_watermark (topic, partition, reviewed_offset, reviewed_by, reviewed_at)
                 VALUES (?, ?, ?, ?, now())
            ON CONFLICT (topic, partition) DO UPDATE
                    SET reviewed_offset = GREATEST(dlq_triage_watermark.reviewed_offset, EXCLUDED.reviewed_offset),
                        reviewed_by     = CASE WHEN EXCLUDED.reviewed_offset > dlq_triage_watermark.reviewed_offset
                                               THEN EXCLUDED.reviewed_by ELSE dlq_triage_watermark.reviewed_by END,
                        reviewed_at     = CASE WHEN EXCLUDED.reviewed_offset > dlq_triage_watermark.reviewed_offset
                                               THEN now() ELSE dlq_triage_watermark.reviewed_at END
              RETURNING reviewed_offset""";

    private static final String SELECT_REVIEWED_OFFSET =
            "SELECT reviewed_offset FROM dlq_triage_watermark WHERE topic = ? AND partition = ?";

    private final JdbcTemplate jdbc;

    public DlqTriageWatermarkDao(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Advances the reviewed high-water for {@code (topic, partition)} to {@code max(current, reviewedOffset)}.
     * Idempotent and monotonic — a lower-or-equal offset leaves the stored high-water (and its audit
     * columns) unchanged.
     *
     * @return the resulting reviewed high-water after the (possibly no-op) advance
     */
    public long advance(String topic, int partition, long reviewedOffset, String reviewedBy) {
        Long result = jdbc.queryForObject(ADVANCE, Long.class, topic, partition, reviewedOffset, reviewedBy);
        return result == null ? reviewedOffset : result;   // RETURNING always yields exactly one row here
    }

    /**
     * The reviewed high-water for {@code (topic, partition)}, or empty when no human has reviewed this
     * partition yet — the depth gauge treats empty as "review starts from the partition's beginning
     * offset" (i.e. full-topic depth until an operator records a review).
     */
    public Optional<Long> reviewedOffset(String topic, int partition) {
        return jdbc.query(SELECT_REVIEWED_OFFSET,
                rs -> rs.next() ? Optional.of(rs.getLong(1)) : Optional.empty(),
                topic, partition);
    }
}
