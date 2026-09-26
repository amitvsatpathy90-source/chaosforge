package io.chaosforge.controlplane.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.chaosforge.schema.v1.ScenarioRunResult;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;

/** Real embedded broker: results.v1 -> RunProjectionListener -> run_projection row. */
class RunProjectionListenerIT extends AbstractCpIntegrationTest {

    // any byte[] producer bean serves as a raw publisher here
    @Autowired
    private KafkaTemplate<String, byte[]> outboxKafkaTemplate;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private MeterRegistry meterRegistry;

    @Test
    void wellFormedResult_landsInRunProjection() {
        UUID scenarioId = UUID.randomUUID();
        UUID tenantId = UUID.randomUUID();
        Instant finishedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        publish(scenarioId, tenantId, 1L, "COMPLETED", finishedAt);

        awaitTrue(Duration.ofSeconds(10), () -> rowCount(scenarioId) == 1);
        assertThat(outcome(scenarioId)).isEqualTo("COMPLETED");
    }

    @Test
    void malformedBytes_skipDoesNotStallPartition_andIncrementsCounter() {
        double before = decodeFailureCount();
        UUID scenarioId = UUID.randomUUID();
        UUID tenantId = UUID.randomUUID();

        outboxKafkaTemplate.send(new ProducerRecord<>("chaosforge.scenario.results.v1",
                UUID.randomUUID().toString(), new byte[] {0x00, (byte) 0xFF, 0x01}));
        // A good record right after proves the partition isn't stuck behind the poison one.
        publish(scenarioId, tenantId, 1L, "FAILED", Instant.now().truncatedTo(ChronoUnit.MILLIS));

        awaitTrue(Duration.ofSeconds(10), () -> rowCount(scenarioId) == 1);
        assertThat(decodeFailureCount()).isGreaterThan(before);
    }

    private void publish(UUID scenarioId, UUID tenantId, long replayVersion, String outcome, Instant finishedAt) {
        try {
            ScenarioRunResult event = ScenarioRunResult.newBuilder()
                    .setScenarioId(scenarioId.toString())
                    .setTenantId(tenantId.toString())
                    .setReplayVersion(replayVersion)
                    .setOutcome(outcome)
                    .setFinishedAt(finishedAt)
                    .build();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            new SpecificDatumWriter<ScenarioRunResult>(ScenarioRunResult.getClassSchema(),
                    event.getSpecificData()).write(event, encoder);
            encoder.flush();
            outboxKafkaTemplate.send(new ProducerRecord<>("chaosforge.scenario.results.v1",
                    scenarioId.toString(), out.toByteArray()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int rowCount(UUID scenarioId) {
        return jdbc.queryForObject(
                "SELECT count(*) FROM run_projection WHERE scenario_id = ?", Integer.class, scenarioId);
    }

    private String outcome(UUID scenarioId) {
        return jdbc.queryForObject(
                "SELECT outcome FROM run_projection WHERE scenario_id = ?", String.class, scenarioId);
    }

    private double decodeFailureCount() {
        return meterRegistry.get("chaosforge.run_projection.decode_failures").counter().count();
    }

    private void awaitTrue(Duration timeout, java.util.function.BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        throw new AssertionError("condition not met within " + timeout);
    }
}
