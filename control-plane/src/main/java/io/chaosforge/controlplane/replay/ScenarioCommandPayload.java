package io.chaosforge.controlplane.replay;

import java.time.Instant;
import java.util.UUID;

/** Logical command captured at replay-claim time; encoded to the outbox payload by {@link CommandPayloadCodec}. */
public record ScenarioCommandPayload(
        UUID scenarioId,
        UUID tenantId,
        long replayVersion,
        UUID ruleSetId,
        int ruleSetVersion,
        Instant issuedAt) {
}
