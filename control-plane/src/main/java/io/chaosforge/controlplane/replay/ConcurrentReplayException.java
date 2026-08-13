package io.chaosforge.controlplane.replay;

import java.util.UUID;

/**
 * The CAS predicate matched 0 rows: another replay already advanced {@code replay_version}, or the
 * supplied {@code expectedVersion} (the client's {@code If-Match}) was stale. Mapped to
 * {@code HTTP 409} with {@code Retry-After: 2} (ADR-0522). The message carries no tenant PII.
 */
public class ConcurrentReplayException extends RuntimeException {
    public ConcurrentReplayException(UUID scenarioId, long expectedVersion) {
        super("concurrent replay for scenario " + scenarioId + " (stale expectedVersion=" + expectedVersion + ")");
    }
}
