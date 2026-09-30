package io.chaosforge.controlplane.pipeline;

import io.chaosforge.schema.v1.ScenarioRunResult;
import java.io.IOException;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.springframework.stereotype.Component;

@Component
public class AvroResultPayloadDecoder implements ResultPayloadDecoder {

    private static final DatumReader<ScenarioRunResult> READER = new SpecificDatumReader<>(
            ScenarioRunResult.getClassSchema(), ScenarioRunResult.getClassSchema(),
            new ScenarioRunResult().getSpecificData());

    @Override
    public ScenarioRunResult decode(byte[] payload) {
        if (payload == null) {
            throw new IllegalArgumentException("null result payload");   // caught + logged by the listener, not DLQ-routed
        }
        try {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(payload, null);
            return READER.read(null, decoder);
        } catch (IOException | RuntimeException e) {
            // Broad catch: corrupt bytes throw an open-ended set — all fold into log-and-skip here.
            throw new IllegalStateException("Avro decode of ScenarioRunResult failed", e);
        }
    }
}
