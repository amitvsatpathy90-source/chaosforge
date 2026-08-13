package io.chaosforge.execution.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import io.chaosforge.schema.v1.ScenarioRunResult;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.jupiter.api.Test;

/**
 * The result-event wire contract (ADR-0525 on {@code chaosforge.scenario.results.v1}): what
 * {@link AvroResultPayloadCodec} writes at outbox-insert time must decode losslessly with the
 * generated {@link ScenarioRunResult} reader — the same decode any downstream results consumer will
 * run at its step 1. This replaced the ADR-0523 Phase-3 placeholder ({@code outcome.getBytes()}),
 * which no Avro consumer could ever have read.
 */
class AvroResultPayloadCodecTest {

    private final AvroResultPayloadCodec codec = new AvroResultPayloadCodec();

    @Test
    void encodedResult_decodesLosslessly_withTheGeneratedReader() throws IOException {
        UUID scenarioId = UUID.randomUUID();
        UUID tenantId = UUID.randomUUID();
        // timestamp-millis logical type: sub-millisecond precision is truncated on the wire by design.
        Instant finishedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        byte[] wire = codec.encode(
                new ExecutionResult(scenarioId, tenantId, 7L, "INCOMPLETE"), finishedAt);
        ScenarioRunResult decoded = decode(wire);

        assertThat(decoded.getScenarioId().toString()).isEqualTo(scenarioId.toString());
        assertThat(decoded.getTenantId().toString()).isEqualTo(tenantId.toString());
        assertThat(decoded.getReplayVersion()).isEqualTo(7L);
        assertThat(decoded.getOutcome().toString()).isEqualTo("INCOMPLETE");
        assertThat(decoded.getFinishedAt()).isEqualTo(finishedAt);
    }

    @Test
    void everyTerminalOutcome_isRepresentable() throws IOException {
        // outcome is a string, not an enum, precisely so new terminal states never need a schema bump.
        for (String outcome : new String[] {"COMPLETED", "FAILED", "ABORTED", "INCOMPLETE"}) {
            byte[] wire = codec.encode(
                    new ExecutionResult(UUID.randomUUID(), UUID.randomUUID(), 1L, outcome), Instant.now());
            assertThat(decode(wire).getOutcome().toString()).isEqualTo(outcome);
        }
    }

    private static ScenarioRunResult decode(byte[] wire) throws IOException {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(wire, null);
        return new SpecificDatumReader<ScenarioRunResult>(ScenarioRunResult.getClassSchema(),
                ScenarioRunResult.getClassSchema(), new ScenarioRunResult().getSpecificData())
                .read(null, decoder);
    }
}
