package io.chaosforge.execution.dlq;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Pure decision-core coverage for the DLQ retry policy (dlq-rules.md). No broker, no sleep — the whole
 * replayability taxonomy and the backoff curve are asserted here.
 */
class DlqRetryPolicyTest {

    // max-attempts 5, base 1s, cap 30s
    private final DlqRetryPolicy policy = new DlqRetryPolicy(5, 1_000L, 30_000L);

    @Test
    void hardPoisonReasons_areSkipped_neverReplayed() {
        for (String reason : new String[] {"SCHEMA_INVALID", "FENCING_VIOLATION", "STEP_FAILED", "RETRY_EXHAUSTED"}) {
            assertThat(policy.decide(reason, 0))
                    .as("hard poison %s must be skipped", reason)
                    .isInstanceOf(RetryDecision.Skip.class);
        }
    }

    @Test
    void unknownOrMissingReason_failsClosedToSkip() {
        assertThat(policy.decide(null, 0)).isInstanceOf(RetryDecision.Skip.class);
        assertThat(policy.decide("WHO_KNOWS", 0)).isInstanceOf(RetryDecision.Skip.class);
    }

    @Test
    void replayableReasons_underBudget_republishWithIncrementedAttempt() {
        for (String reason : new String[] {"INFRA_TRANSIENT", "STEP_TIMEOUT"}) {
            assertThat(policy.decide(reason, 0)).isInstanceOfSatisfying(RetryDecision.Republish.class, r -> {
                assertThat(r.nextAttempt()).isEqualTo(1);
                assertThat(r.backoffMillis()).isEqualTo(1_000L);   // base on the first retry
            });
        }
    }

    @Test
    void backoffIsExponential_andCapped_withoutOverflow() {
        assertThat(policy.backoffMillis(1)).isEqualTo(1_000L);
        assertThat(policy.backoffMillis(2)).isEqualTo(2_000L);
        assertThat(policy.backoffMillis(3)).isEqualTo(4_000L);
        assertThat(policy.backoffMillis(4)).isEqualTo(8_000L);
        assertThat(policy.backoffMillis(5)).isEqualTo(16_000L);
        assertThat(policy.backoffMillis(6)).isEqualTo(30_000L);    // clamped to cap
        assertThat(policy.backoffMillis(1_000)).isEqualTo(30_000L); // stays capped, no shift overflow
    }

    @Test
    void lastAllowedAttempt_stillRepublishes() {
        // currentAttempt 4 → nextAttempt 5 == max-attempts → still a republish
        assertThat(policy.decide("STEP_TIMEOUT", 4)).isInstanceOfSatisfying(
                RetryDecision.Republish.class, r -> assertThat(r.nextAttempt()).isEqualTo(5));
    }

    @Test
    void budgetSpent_exhausts() {
        // currentAttempt 5 → nextAttempt 6 > max-attempts 5 → Exhausted
        assertThat(policy.decide("INFRA_TRANSIENT", 5)).isInstanceOfSatisfying(
                RetryDecision.Exhausted.class, e -> assertThat(e.attempts()).isEqualTo(5));
    }
}
