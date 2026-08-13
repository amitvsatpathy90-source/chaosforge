package io.chaosforge.execution.pipeline;

import io.chaosforge.execution.dlq.DlqRoutableException;
import io.chaosforge.execution.persistence.InboxDao;
import io.chaosforge.execution.persistence.InboxFenceDao;
import io.chaosforge.execution.persistence.RunLogDao;
import io.chaosforge.schema.v1.ScenarioRunCommand;
import java.util.UUID;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Phase 1 — short claim transaction (ADR-0523). Fencing PRECEDES inbox dedup (load-bearing): a stale
 * version is structurally invalid, not a duplicate. Stale → FENCING_VIOLATION (tx rolls back, → DLQ).
 *
 * <p>Stale test is {@code incomingVersion < maxSeen} (ADR-0523), NOT {@code <=}: a same-version
 * redelivery must fall through to inbox dedup (silent ack-skip), not be DLQ'd. (execution-service-rules
 * shows {@code <=}, which would route every redelivery to the DLQ — that is the wrong boundary.)
 */
@Component
public class ClaimPhase {

    private final InboxFenceDao fenceDao;
    private final InboxDao inboxDao;
    private final RunLogDao runLogDao;

    public ClaimPhase(InboxFenceDao fenceDao, InboxDao inboxDao, RunLogDao runLogDao) {
        this.fenceDao = fenceDao;
        this.inboxDao = inboxDao;
        this.runLogDao = runLogDao;
    }

    @Transactional(timeout = 5)
    public ClaimResult claim(ScenarioRunCommand command, UUID messageId, long incomingVersion) {
        UUID scenarioId = UUID.fromString(String.valueOf(command.getScenarioId()));
        UUID tenantId = UUID.fromString(String.valueOf(command.getTenantId()));

        // Step 2 — fencing (FOR UPDATE), before inbox.
        long maxSeen = fenceDao.lockAndGetMaxSeen(scenarioId, tenantId);
        if (incomingVersion < maxSeen) {
            throw DlqRoutableException.fencingViolation(
                    "stale replay_version " + incomingVersion + " < max_seen " + maxSeen);
        }
        fenceDao.advanceMaxSeen(scenarioId, incomingVersion);

        // Step 3 — inbox dedup (same tx as the business effect).
        if (!inboxDao.insertIfAbsent(messageId, tenantId, scenarioId)) {
            return ClaimResult.duplicate();
        }
        runLogDao.insertRunHeader(scenarioId, incomingVersion, tenantId);
        return ClaimResult.proceed();
    }
}
