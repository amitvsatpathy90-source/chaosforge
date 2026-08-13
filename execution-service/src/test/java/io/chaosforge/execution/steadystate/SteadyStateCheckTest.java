package io.chaosforge.execution.steadystate;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * The steady-state breach logic (acceptance gate C20, time-based per ADR-0533): within one boundary
 * evaluation the target is probed at a fixed interval until it comes back healthy (streak resets, no
 * breach) or {@code maxConsecutiveFailures} probes fail in a row (breach). This is independent of step
 * count, so a single-step scenario can still abort — the defect arch-audit 2.3 found. Probing is bounded
 * by the aggregate deadline. Sleeps are stubbed so the interval loop runs without wall-clock delay.
 */
class SteadyStateCheckTest {

    private static final long FAR_DEADLINE = Long.MAX_VALUE;
    private static final SteadyStateCheck.Sleeper NO_SLEEP = millis -> { };

    private final AtomicInteger breaches = new AtomicInteger();

    @Test
    void healthyTarget_neverBreaches() {
        SteadyStateCheck check = check(responses(true), 3);
        for (int i = 0; i < 10; i++) {
            assertThat(check.breached(FAR_DEADLINE)).isFalse();
        }
        assertThat(breaches).hasValue(0);
    }

    @Test
    void tolerance1_breachesOnFirstUnhealthyProbe() {
        SteadyStateCheck check = check(responses(false), 1);
        assertThat(check.breached(FAR_DEADLINE)).isTrue();
        assertThat(breaches).hasValue(1);
    }

    @Test
    void tolerance3_breachesAfterThreeConsecutiveFailures_withinOneEvaluation() {
        // A single-step scenario gets exactly one boundary evaluation — it must still be able to breach.
        SteadyStateCheck check = check(responses(false), 3);
        assertThat(check.breached(FAR_DEADLINE)).isTrue();
        assertThat(breaches).hasValue(1);
    }

    @Test
    void aHealthyProbeResetsTheStreak_soTransientBlipsDoNotAbort() {
        // fail, fail, recover → the healthy probe ends the evaluation with no breach.
        SteadyStateCheck check = check(responses(false, false, true), 3);
        assertThat(check.breached(FAR_DEADLINE)).isFalse();
        assertThat(breaches).hasValue(0);
    }

    @Test
    void deadlineAlreadyExpired_yieldsWithoutProbing() {
        // arch-audit M1: the deadline is guarded at the TOP of the loop, before allHealthy(). A probe
        // pass costs wall-clock (the probe read timeout per host), so entering it with no budget left is
        // exactly how the loop used to overshoot the aggregate deadline. Already out of budget on entry →
        // do not probe at all, yield to the aggregate-deadline control.
        AtomicInteger probes = new AtomicInteger();
        SteadyStateCheck check = new SteadyStateCheck(
                List.of("http://a/health", "http://b/health", "http://c/health"),
                url -> { probes.incrementAndGet(); return false; },   // always unhealthy
                100, 1_000, breaches::incrementAndGet, NO_SLEEP);

        assertThat(check.breached(0L)).isFalse();
        assertThat(breaches).hasValue(0);
        assertThat(probes).as("no probe pass is entered once the deadline is spent").hasValue(0);
    }

    @Test
    void disabled_emptyHealthUrls_neverBreaches_andNeverProbes() {
        AtomicInteger probes = new AtomicInteger();
        SteadyStateCheck check = new SteadyStateCheck(
                List.of(), url -> { probes.incrementAndGet(); return false; }, 1, 0,
                breaches::incrementAndGet, NO_SLEEP);
        assertThat(check.breached(FAR_DEADLINE)).isFalse();
        assertThat(probes).as("a disabled check does not even probe").hasValue(0);
    }

    private SteadyStateCheck check(TargetHealthProbe probe, int maxConsecutiveFailures) {
        return new SteadyStateCheck(List.of("http://target/health"), probe, maxConsecutiveFailures,
                0, breaches::incrementAndGet, NO_SLEEP);
    }

    /** A probe that returns the given sequence of health results, repeating the last one thereafter. */
    private static TargetHealthProbe responses(boolean... healthy) {
        AtomicInteger i = new AtomicInteger();
        return url -> healthy[Math.min(i.getAndIncrement(), healthy.length - 1)];
    }
}
