package io.chaosforge.execution.pipeline;

import io.chaosforge.schema.v1.ScenarioRunResult;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.springframework.stereotype.Component;

/**
 * Encodes the result event to Avro binary at outbox-insert time (schema-at-write, ADR-0525) —
 * {@code chaosforge.scenario.results.v1} is a canonical topic, so its wire format is the registered
 * {@link ScenarioRunResult} schema, never ad-hoc bytes (this replaces the ADR-0523 Phase-3
 * placeholder payload).
 *
 * <p><b>Why local encoding, not the Apicurio serializer:</b> the encode runs inside the Phase-3 /
 * sweeper transaction, where external HTTP is forbidden; the Apicurio serializer performs registry
 * I/O on serialize. A local {@code SpecificDatumWriter} is pure CPU (mirrors the CP
 * {@code AvroCommandPayloadCodec} — ADR-0525). Registry-side FULL_TRANSITIVE enforcement happens at
 * build time via {@code avroSchemaCompatibilityCheck} (C29), which now also gates this subject.
 */
@Component
public class AvroResultPayloadCodec implements ResultPayloadCodec {

    private static final DatumWriter<ScenarioRunResult> WRITER =
            new SpecificDatumWriter<>(ScenarioRunResult.getClassSchema(), new ScenarioRunResult().getSpecificData());

    @Override
    public byte[] encode(ExecutionResult result, Instant finishedAt) {
        ScenarioRunResult event = ScenarioRunResult.newBuilder()
                .setScenarioId(result.scenarioId().toString())
                .setTenantId(result.tenantId().toString())
                .setReplayVersion(result.replayVersion())
                .setOutcome(result.outcome())
                .setFinishedAt(finishedAt)
                .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        try {
            WRITER.write(event, encoder);
            encoder.flush();
        } catch (IOException e) {
            // Serialization failure rolls back the finalize/sweep tx → row never committed → the
            // relay never ships an unserializable result event.
            throw new UncheckedIOException("Avro encode of ScenarioRunResult failed", e);
        }
        return out.toByteArray();
    }
}
