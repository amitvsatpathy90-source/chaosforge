package io.chaosforge.controlplane.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.chaosforge.schema.v1.ScenarioRunResult;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

/**
 * CP's first ScenarioRunResult decode path. Must read what exec's AvroResultPayloadCodec writes —
 * this test builds the wire bytes independently (no cross-module dependency on execution-service)
 * so it stands as the contract check on this side of the wire.
 */
class AvroResultPayloadDecoderTest {

    private final AvroResultPayloadDecoder decoder = new AvroResultPayloadDecoder();

    @Test
    void wellFormedPayload_decodesAllFields() throws IOException {
        UUID scenarioId = UUID.randomUUID();
        UUID tenantId = UUID.randomUUID();
        Instant finishedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);   // timestamp-millis logical type

        byte[] wire = encode(scenarioId, tenantId, 3L, "COMPLETED", finishedAt);
        ScenarioRunResult result = decoder.decode(wire);

        assertThat(result.getScenarioId().toString()).isEqualTo(scenarioId.toString());
        assertThat(result.getTenantId().toString()).isEqualTo(tenantId.toString());
        assertThat(result.getReplayVersion()).isEqualTo(3L);
        assertThat(result.getOutcome().toString()).isEqualTo("COMPLETED");
        assertThat(result.getFinishedAt()).isEqualTo(finishedAt);
    }

    @Test
    void nullPayload_throws() {
        assertThatThrownBy(() -> decoder.decode(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void corruptBytes_throw() {
        byte[] garbage = {0x00, (byte) 0xFF, 0x12, 0x34, 0x56};
        assertThatThrownBy(() -> decoder.decode(garbage)).isInstanceOf(IllegalStateException.class);
    }

    private static byte[] encode(UUID scenarioId, UUID tenantId, long replayVersion,
                                 String outcome, Instant finishedAt) throws IOException {
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
                new ScenarioRunResult().getSpecificData()).write(event, encoder);
        encoder.flush();
        return out.toByteArray();
    }
}
