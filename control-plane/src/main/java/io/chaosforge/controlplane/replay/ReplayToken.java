package io.chaosforge.controlplane.replay;

import java.util.UUID;

/**
 * Result of a successful replay claim (ADR-0528). {@code fencingToken} is the new, monotonic
 * {@code replay_version}.
 *
 * <p>{@code idempotentReplay} distinguishes the two success paths so the controller can pick the
 * status code: a fresh claim → {@code 202 Accepted}; a COMPLETED-key replay that returns the
 * originally-stored token → {@code 200 OK}.
 */
public record ReplayToken(
        UUID scenarioId,
        long fencingToken,          // == new replay_version
        UUID ruleSetId,
        int ruleSetVersion,
        UUID outboxMessageId,       // UUIDv7, for trace correlation
        boolean idempotentReplay) {

    /** A freshly-claimed run (HTTP 202). */
    public static ReplayToken claimed(UUID scenarioId, long fencingToken, UUID ruleSetId,
                                      int ruleSetVersion, UUID outboxMessageId) {
        return new ReplayToken(scenarioId, fencingToken, ruleSetId, ruleSetVersion, outboxMessageId, false);
    }

    /** An idempotent replay of a COMPLETED key — original token returned (HTTP 200). */
    public static ReplayToken idempotentHit(UUID scenarioId, long fencingToken, UUID ruleSetId,
                                            int ruleSetVersion, UUID outboxMessageId) {
        return new ReplayToken(scenarioId, fencingToken, ruleSetId, ruleSetVersion, outboxMessageId, true);
    }
}
