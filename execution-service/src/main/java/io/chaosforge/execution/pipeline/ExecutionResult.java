package io.chaosforge.execution.pipeline;

import java.util.UUID;

/** Result of Phase 2 execution. {@code outcome} is COMPLETED or FAILED. */
public record ExecutionResult(UUID scenarioId, UUID tenantId, long replayVersion, String outcome) {
}
