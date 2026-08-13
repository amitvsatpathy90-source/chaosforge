package io.chaosforge.execution.steadystate;

import java.util.List;

/**
 * Steady-state hypothesis for one scenario run (C20): probes target health between fault steps,
 * breaches after {@code maxConsecutiveFailures} consecutive unhealthy probes → auto-abort.
 * Time-based, not per-step (ADR-0533): probes at a fixed interval until healthy (streak resets) or
 * breached, so short scenarios can still abort. Loop is bounded by the aggregate deadline.
 * Stateful, single-use per run; empty healthUrls means disabled and never breaches.
 */
public final class SteadyStateCheck {

    /** Injectable sleep so tests can drive the interval loop without real wall-clock delay. */
    @FunctionalInterface
    public interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private final List<String> healthUrls;
    private final TargetHealthProbe probe;
    private final int maxConsecutiveFailures;
    private final long probeIntervalMs;
    private final Runnable onBreach;
    private final Sleeper sleeper;

    SteadyStateCheck(List<String> healthUrls, TargetHealthProbe probe, int maxConsecutiveFailures,
                     long probeIntervalMs, Runnable onBreach, Sleeper sleeper) {
        this.healthUrls = List.copyOf(healthUrls);
        this.probe = probe;
        this.maxConsecutiveFailures = Math.max(1, maxConsecutiveFailures);
        this.probeIntervalMs = Math.max(0, probeIntervalMs);
        this.onBreach = onBreach;
        this.sleeper = sleeper;
    }

    /**
     * Evaluate steady state at a step boundary, probing at {@code probeIntervalMs} until resolved.
     *
     * @param deadlineMs epoch-millis aggregate deadline; probing stops (no breach) if the next sleep
     *                   would cross it, so the deadline control — not this loop — bounds the run.
     * @return true once the steady state has been breached — the caller must abort the run.
     */
    public boolean breached(long deadlineMs) {
        if (healthUrls.isEmpty()) {
            return false;   // disabled, or no probe-able target
        }
        int consecutiveFailures = 0;
        while (true) {
            // Guard before probing, not just before sleeping (arch-audit M1) — a probe pass costs
            // wall-clock too; out of budget → no breach, ExecutePhase's deadline control takes over.
            if (System.currentTimeMillis() >= deadlineMs) {
                return false;
            }
            if (allHealthy()) {
                return false;   // recovered / healthy — streak resets, no breach
            }
            if (++consecutiveFailures >= maxConsecutiveFailures) {
                onBreach.run();
                return true;
            }
            if (System.currentTimeMillis() + probeIntervalMs >= deadlineMs) {
                return false;   // next sleep would cross the deadline — let the aggregate control take over
            }
            try {
                sleeper.sleep(probeIntervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    /** The steady-state hypothesis holds iff every derived target is healthy. */
    private boolean allHealthy() {
        for (String url : healthUrls) {
            if (!probe.isHealthy(url)) {
                return false;
            }
        }
        return true;
    }

    public List<String> healthUrls() {
        return healthUrls;
    }
}
