package io.chaosforge.controlplane.it;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.chaosforge.controlplane.dlq.DlqDepthGauge;
import io.chaosforge.controlplane.dlq.DlqTriageWatermarkDao;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Proves the standing DLQ-depth gauge (ADR-0542, module 2) end-to-end against a real broker (embedded
 * Kafka) and real Postgres: {@code chaosforge.dlq.untriaged_depth} tracks {@code endOffset − reviewed
 * high-water}, moving down only as an operator advances the watermark — the signal arch-audit F5 found
 * missing. The gauge's consumer is read-only (no {@code group.id}), so measuring depth never disturbs
 * the retry consumer's group offset.
 */
class DlqDepthGaugeIT extends AbstractCpIntegrationTest {

    // The configured default DLQ topic — using it keeps this IT on the shared cached context (no
    // property override) and exercises the real default rather than a synthetic topic.
    private static final String DLQ_TOPIC = "chaosforge.scenario.commands.v1.DLQ";

    @Autowired
    private DlqDepthGauge depthGauge;

    @Autowired
    private DlqTriageWatermarkDao watermarks;

    @Autowired
    private KafkaTemplate<String, byte[]> outboxKafkaTemplate;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private JdbcTemplate jdbc;

    @BeforeEach
    void cleanWatermark() {
        // Shared cached context with other watermark ITs — start from "nothing reviewed" regardless of
        // test order (not in AbstractCpIntegrationTest's truncate list).
        jdbc.update("TRUNCATE dlq_triage_watermark");
    }

    @Test
    void depthTracksEndOffsetMinusWatermark() throws Exception {
        // Five poison records on partition 0 → offsets 0..4, end offset = 5.
        long lastOffset = -1L;
        for (int i = 0; i < 5; i++) {
            lastOffset = outboxKafkaTemplate
                    .send(new ProducerRecord<>(DLQ_TOPIC, 0, "k", ("poison-" + i).getBytes(UTF_8)))
                    .get().getRecordMetadata().offset();
        }
        assertThat(lastOffset).isEqualTo(4L);   // end offset (next to be produced) = 5

        // No watermark yet → nothing reviewed → full-topic depth = 5.
        depthGauge.refresh();
        assertThat(depthOnPartition(0)).isEqualTo(5.0);

        // Operator reviews up to offset 3 (records 0,1,2 dispositioned) → depth = 5 − 3 = 2.
        watermarks.advance(DLQ_TOPIC, 0, 3L, "operator-a");
        depthGauge.refresh();
        assertThat(depthOnPartition(0)).isEqualTo(2.0);

        // Reviewed through the end offset → backlog drained to 0 (records still retained in Kafka).
        watermarks.advance(DLQ_TOPIC, 0, 5L, "operator-a");
        depthGauge.refresh();
        assertThat(depthOnPartition(0)).isEqualTo(0.0);

        // A stale (lower) advance is a no-op on the watermark, so depth does not move.
        watermarks.advance(DLQ_TOPIC, 0, 1L, "operator-b");
        depthGauge.refresh();
        assertThat(depthOnPartition(0)).isEqualTo(0.0);
    }

    private double depthOnPartition(int partition) {
        return meterRegistry.get("chaosforge.dlq.untriaged_depth")
                .tags("topic", DLQ_TOPIC, "partition", Integer.toString(partition))
                .gauge().value();
    }
}
