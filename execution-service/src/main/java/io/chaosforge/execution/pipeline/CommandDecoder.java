package io.chaosforge.execution.pipeline;

import com.github.f4b6a3.uuid.util.UuidUtil;
import io.chaosforge.execution.dlq.DlqRoutableException;
import io.chaosforge.schema.v1.ScenarioRunCommand;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Pipeline step 1: Avro decode + tenant verification + message-id mint-clock sanity. The signed Avro
 * {@code tenantId} is the authoritative tenant on the Kafka path (mtls-rules.md); the {@code x-tenant-id}
 * header must match it. Any failure → SCHEMA_INVALID → DLQ (not replayable).
 */
@Component
public class CommandDecoder {

    private static final DatumReader<ScenarioRunCommand> READER = new SpecificDatumReader<>(
            ScenarioRunCommand.getClassSchema(), ScenarioRunCommand.getClassSchema(),
            new ScenarioRunCommand().getSpecificData());

    private final Duration maxPast;
    private final Duration maxFuture;

    public CommandDecoder(
            @Value("${chaosforge.partition.inbox-retention-days:7}") int inboxRetentionDays,
            @Value("${chaosforge.partition.lookahead-days:4}") int lookaheadDays) {
        this.maxPast = Duration.ofDays(inboxRetentionDays);
        this.maxFuture = Duration.ofDays(lookaheadDays);
    }

    public ScenarioRunCommand decode(byte[] payload) {
        if (payload == null) {
            throw DlqRoutableException.schemaInvalid("null command payload", null);
        }
        try {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(payload, null);
            return READER.read(null, decoder);
        } catch (IOException | RuntimeException e) {
            // Broad catch: corrupt bytes throw an open-ended set; all must become SCHEMA_INVALID.
            throw DlqRoutableException.schemaInvalid("Avro decode failed", e);
        }
    }

    public void verifyTenant(ScenarioRunCommand command, String tenantHeader) {
        String signedTenant = String.valueOf(command.getTenantId());
        if (tenantHeader == null || !tenantHeader.equals(signedTenant)) {
            throw DlqRoutableException.schemaInvalid("x-tenant-id does not match signed payload tenant", null);
        }
    }

    /**
     * Rejects a message_id whose embedded UUIDv7 timestamp falls outside the partition window
     * [now-inbox-retention, now+lookahead] (partitioning-rules §1) — otherwise it would cascade
     * into the un-droppable inbox_default partition. Checked before any DB write.
     */
    public void verifyMintClock(UUID messageId) {
        Instant msgTs;
        try {
            msgTs = UuidUtil.getInstant(messageId);
        } catch (RuntimeException e) {
            // Not a time-ordered (v7) UUID → no derivable partition timestamp → structurally invalid.
            throw DlqRoutableException.schemaInvalid("message_id is not a time-ordered UUID", e);
        }
        Instant now = Instant.now();
        if (msgTs.isBefore(now.minus(maxPast)) || msgTs.isAfter(now.plus(maxFuture))) {
            // Shape token only — never the raw timestamp/id beyond what's needed to triage (PII rule).
            throw DlqRoutableException.schemaInvalid("message_id mint time outside partition window", null);
        }
    }
}
