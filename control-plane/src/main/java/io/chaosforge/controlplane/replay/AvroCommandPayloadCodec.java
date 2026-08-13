package io.chaosforge.controlplane.replay;

import io.chaosforge.schema.v1.ScenarioRunCommand;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.springframework.stereotype.Component;

/**
 * Encodes the command to Avro binary at outbox-insert time (schema-at-write, ADR-0525).
 * Local {@link SpecificDatumWriter}, not the Apicurio serializer — encoding runs inside the CAS
 * tx (ADR-0522), where registry HTTP is forbidden. Apicurio enforces compatibility at build time only.
 */
@Component
public class AvroCommandPayloadCodec implements CommandPayloadCodec {

    private static final DatumWriter<ScenarioRunCommand> WRITER =
            new SpecificDatumWriter<>(ScenarioRunCommand.getClassSchema(), new ScenarioRunCommand().getSpecificData());

    @Override
    public byte[] encode(ScenarioCommandPayload p) {
        ScenarioRunCommand command = ScenarioRunCommand.newBuilder()
                .setScenarioId(p.scenarioId().toString())
                .setTenantId(p.tenantId().toString())
                .setReplayVersion(p.replayVersion())
                .setRuleSetId(p.ruleSetId().toString())
                .setRuleSetVersion(p.ruleSetVersion())
                .setSteps(List.of())               // consumer loads steps from the pinned rule set (determinism, ADR-0503)
                .setCommandIssuedAt(p.issuedAt())
                .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        try {
            WRITER.write(command, encoder);
            encoder.flush();
        } catch (IOException e) {
            // Serialization failure rolls back the CAS tx → row never committed → poller never ships it.
            throw new UncheckedIOException("Avro encode of ScenarioRunCommand failed", e);
        }
        return out.toByteArray();
    }
}
