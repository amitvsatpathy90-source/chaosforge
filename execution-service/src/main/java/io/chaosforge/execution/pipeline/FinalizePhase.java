package io.chaosforge.execution.pipeline;

import com.github.f4b6a3.uuid.UuidCreator;
import io.chaosforge.execution.persistence.ExecOutboxDao;
import io.chaosforge.execution.persistence.RunLogDao;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Phase 3 — finalize (ADR-0523). Emits the result event via the outbox and finalizes the run header
 * in one short tx. The payload is Avro binary ({@link AvroResultPayloadCodec} —
 * {@code ScenarioRunResult}, ADR-0525): results is a canonical topic, schema-at-write like the
 * command path.
 */
@Component
public class FinalizePhase {

    private static final Logger log = LoggerFactory.getLogger(FinalizePhase.class);

    /** Canonical results topic; also referenced by the INCOMPLETE-run sweeper's terminal events. */
    public static final String RESULT_TOPIC = "chaosforge.scenario.results.v1";

    private final ExecOutboxDao outboxDao;
    private final RunLogDao runLogDao;
    private final ResultPayloadCodec payloadCodec;

    public FinalizePhase(ExecOutboxDao outboxDao, RunLogDao runLogDao, ResultPayloadCodec payloadCodec) {
        this.outboxDao = outboxDao;
        this.runLogDao = runLogDao;
        this.payloadCodec = payloadCodec;
    }

    @Transactional(timeout = 5)
    public void complete(ExecutionResult result) {
        // Flip first, event second, one tx (arch-audit H2). 1 row = won, emit; 0 = already terminal.
        int flipped = runLogDao.finalizeRun(
                result.scenarioId(), result.replayVersion(), result.outcome(), result.outcome());
        if (flipped == 1) {
            insertResultEvent(result, Instant.now());
        } else {
            log.debug("finalize no-op for scenario {} v{} — run already terminal (swept or duplicate)",
                    result.scenarioId(), result.replayVersion());
        }
    }

    /**
     * Inserts the Avro result event into the outbox. Joins the <b>caller's</b> transaction — used by
     * {@link #complete} (Phase 3) and by the INCOMPLETE-run sweeper, so the topic / headers / payload
     * contract of {@code chaosforge.scenario.results.v1} is defined exactly once.
     */
    public void insertResultEvent(ExecutionResult result, Instant finishedAt) {
        UUID messageId = UuidCreator.getTimeOrderedEpoch();
        byte[] payload = payloadCodec.encode(result, finishedAt);
        String headers = String.format("{\"x-tenant-id\":\"%s\",\"x-replay-version\":\"%d\"}",
                result.tenantId(), result.replayVersion());
        outboxDao.insertResultEvent(messageId, result.scenarioId(), result.tenantId(), result.replayVersion(),
                RESULT_TOPIC, result.scenarioId().toString(), payload, headers);
    }
}
