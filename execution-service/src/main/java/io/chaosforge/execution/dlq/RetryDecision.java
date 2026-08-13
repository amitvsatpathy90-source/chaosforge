package io.chaosforge.execution.dlq;

/**
 * Outcome of the DLQ retry policy for one dead-lettered record (dlq-rules.md). Sealed so the consumer
 * must handle every disposition via pattern matching — a new case cannot be silently dropped.
 */
public sealed interface RetryDecision {

    /** Hard-poison reason (or unknown/missing) — leave it in the DLQ for human triage. */
    record Skip(String reason) implements RetryDecision {}

    /** Replayable and under the attempt budget — republish to the main topic after {@code backoffMillis}. */
    record Republish(int nextAttempt, long backoffMillis) implements RetryDecision {}

    /** Replayable but the attempt budget is spent — park as RETRY_EXHAUSTED for human review. */
    record Exhausted(int attempts) implements RetryDecision {}
}
