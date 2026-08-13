package io.chaosforge.execution.dlq;

/**
 * Carries the {@code x-dlq-reason} for a message that must be dead-lettered (dlq-rules.md). The
 * {@code replayable} flag documents intent; the DLQ retry consumer is the only component that acts
 * on it, and only for INFRA_TRANSIENT / STEP_TIMEOUT.
 */
public class DlqRoutableException extends RuntimeException {

    private final String dlqReason;
    private final boolean replayable;

    private DlqRoutableException(String dlqReason, boolean replayable, String message, Throwable cause) {
        super(message, cause);
        this.dlqReason = dlqReason;
        this.replayable = replayable;
    }

    public String dlqReason() {
        return dlqReason;
    }

    public boolean replayable() {
        return replayable;
    }

    // Step 1 — Avro decode / tenant verification
    public static DlqRoutableException schemaInvalid(String message, Throwable cause) {
        return new DlqRoutableException("SCHEMA_INVALID", false, message, cause);
    }

    // Step 2 — x-replay-version <= max_seen
    public static DlqRoutableException fencingViolation(String message) {
        return new DlqRoutableException("FENCING_VIOLATION", false, message, null);
    }

    // Step 5 — semantic step failure
    public static DlqRoutableException stepFailed(String message) {
        return new DlqRoutableException("STEP_FAILED", false, message, null);
    }

    // Step 5 — CB open / target unreachable after retries (replayable by the retry consumer)
    public static DlqRoutableException infraTransient(String message, Throwable cause) {
        return new DlqRoutableException("INFRA_TRANSIENT", true, message, cause);
    }

    // Step 5 — aggregate step timeout, idempotency-safe (replayable)
    public static DlqRoutableException stepTimeout(String message) {
        return new DlqRoutableException("STEP_TIMEOUT", true, message, null);
    }
}
