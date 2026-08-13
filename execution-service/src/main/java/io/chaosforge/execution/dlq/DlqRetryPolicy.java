package io.chaosforge.execution.dlq;

import java.util.Set;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Pure decision core for the DLQ retry consumer (dlq-rules.md) — no Kafka, no sleep, no I/O, so it is
 * exhaustively unit-testable. Only {@code INFRA_TRANSIENT} and {@code STEP_TIMEOUT} are replayable;
 * widening this set requires an ADR (replay safety rests on step-level idempotency — ADR-0529). The
 * three hard-poison reasons and {@code RETRY_EXHAUSTED} are terminal and never republished.
 */
@Component
public class DlqRetryPolicy {

    static final String RETRY_EXHAUSTED = "RETRY_EXHAUSTED";
    private static final Set<String> REPLAYABLE = Set.of("INFRA_TRANSIENT", "STEP_TIMEOUT");

    private final int maxAttempts;
    private final long baseBackoffMillis;
    private final long capBackoffMillis;

    public DlqRetryPolicy(
            @Value("${chaosforge.dlq.retry.max-attempts:5}") int maxAttempts,
            @Value("${chaosforge.dlq.retry.base-backoff-ms:1000}") long baseBackoffMillis,
            @Value("${chaosforge.dlq.retry.cap-backoff-ms:30000}") long capBackoffMillis) {
        this.maxAttempts = maxAttempts;
        this.baseBackoffMillis = baseBackoffMillis;
        this.capBackoffMillis = capBackoffMillis;
    }

    /**
     * @param reason         the {@code x-dlq-reason} on the record (may be {@code null} if absent)
     * @param currentAttempt the {@code x-dlq-attempt} already on the record (0 if never retried)
     */
    public RetryDecision decide(String reason, int currentAttempt) {
        if (reason == null || !REPLAYABLE.contains(reason)) {
            return new RetryDecision.Skip(reason == null ? "none" : reason);   // hard poison — humans only
        }
        int nextAttempt = currentAttempt + 1;
        if (nextAttempt > maxAttempts) {
            return new RetryDecision.Exhausted(currentAttempt);
        }
        return new RetryDecision.Republish(nextAttempt, backoffMillis(nextAttempt));
    }

    /**
     * Deterministic capped exponential ceiling: {@code base * 2^(attempt-1)}, clamped to {@code cap}.
     * Computed by repeated doubling so it cannot overflow for a misconfigured attempt count. Full
     * jitter is applied by the caller, keeping this ceiling deterministic and testable.
     */
    long backoffMillis(int attempt) {
        long delay = baseBackoffMillis;
        for (int i = 1; i < attempt && delay < capBackoffMillis; i++) {
            delay = Math.min(capBackoffMillis, delay * 2);
        }
        return Math.min(capBackoffMillis, Math.max(0, delay));
    }
}
