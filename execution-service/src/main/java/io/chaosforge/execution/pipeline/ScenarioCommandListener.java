package io.chaosforge.execution.pipeline;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.chaosforge.execution.dlq.DlqRoutableException;
import io.chaosforge.execution.observability.ExecutionMetrics;
import io.chaosforge.schema.v1.ScenarioRunCommand;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionException;

/**
 * The load-bearing pipeline (architecture specifications): decode+tenant (1) → claim=fence+inbox (2,3) → execute (4,5)
 * → finalize=outbox (6) → manual ack (7). Returns void. Ack fires ONLY after Phase 3 commits. Any
 * thrown {@link DlqRoutableException} propagates to the container error handler → DLQ (no ack), so the
 * partition advances past poison and the right {@code x-dlq-reason} is set.
 */
@Component
public class ScenarioCommandListener {

    private final CommandDecoder decoder;
    private final ClaimPhase claimPhase;
    private final ExecutePhase executePhase;
    private final FinalizePhase finalizePhase;
    private final ExecutionMetrics metrics;

    public ScenarioCommandListener(CommandDecoder decoder, ClaimPhase claimPhase,
                                   ExecutePhase executePhase, FinalizePhase finalizePhase,
                                   ExecutionMetrics metrics) {
        this.decoder = decoder;
        this.claimPhase = claimPhase;
        this.executePhase = executePhase;
        this.finalizePhase = finalizePhase;
        this.metrics = metrics;
    }

    @KafkaListener(topics = "${chaosforge.kafka.command-topic}", containerFactory = "kafkaListenerContainerFactory")
    public void onCommand(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        // 1. decode + tenant verification + message-id mint-clock sanity (all SCHEMA_INVALID on failure)
        ScenarioRunCommand command = decoder.decode(record.value());
        decoder.verifyTenant(command, header(record, "x-tenant-id"));
        UUID messageId = parseUuid(header(record, "x-message-id"));
        decoder.verifyMintClock(messageId);   // out-of-window msg_ts would poison inbox_default (partitioning §1)
        long replayVersion = parseVersion(header(record, "x-replay-version"));

        // A transient DB fault (Hikari timeout, PgJDBC socket/statement timeout) is infra, not poison —
        // map it to INFRA_TRANSIENT so the retry lane recovers it (else PG failover strands everything
        // as SCHEMA_INVALID, CHAOS-01). Broad catch: resource-failure subclasses aren't all Transient.
        // A DlqRoutableException thrown inside is a RuntimeException, not caught here — propagates as-is.
        try {
            // Phase 1 — claim (fence + inbox). Stale → FENCING_VIOLATION. Duplicate → ack-skip.
            ClaimResult claim = claimPhase.claim(command, messageId, replayVersion);
            if (claim.isDuplicate()) {
                metrics.deduped();
                ack.acknowledge();
                return;
            }
            // Phase 2 — execute (no tx). May throw INFRA_TRANSIENT / STEP_TIMEOUT / STEP_FAILED → DLQ.
            ExecutionResult result = executePhase.execute(command, replayVersion);
            // Phase 3 — finalize (outbox + run header).
            finalizePhase.complete(result);
        } catch (DataAccessException | TransactionException e) {
            throw DlqRoutableException.infraTransient(
                    "DB fault in execution pipeline for message " + messageId, e);
        }
        metrics.success();
        ack.acknowledge();   // post-Phase-3 commit only
    }

    private static String header(ConsumerRecord<?, ?> record, String key) {
        Header header = record.headers().lastHeader(key);
        if (header == null) {
            throw DlqRoutableException.schemaInvalid("missing header " + key, null);
        }
        return new String(header.value(), UTF_8);
    }

    private static UUID parseUuid(String value) {
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            throw DlqRoutableException.schemaInvalid("bad UUID header", e);
        }
    }

    private static long parseVersion(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw DlqRoutableException.schemaInvalid("bad x-replay-version header", e);
        }
    }
}
