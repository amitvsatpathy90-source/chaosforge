package io.chaosforge.execution.steadystate;

import io.chaosforge.execution.ruleset.RuleSetRef;
import io.chaosforge.execution.ruleset.StepSpec;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Builds the per-run {@link SteadyStateCheck} (acceptance gate C20). The steady-state hypothesis is:
 * "the targets stay healthy while we inject faults"; health is probed at {@code
 * <scheme>://<authority>} + {@code health-path} for <b>every distinct target</b> across the scenario's
 * steps (ADR-0533 — not just the first step's target). When disabled, or when no probe-able target can
 * be derived, the check is inert (never breaches) — existing scenarios are unaffected.
 */
@Component
public class SteadyStateGuard {

    private final TargetHealthProbe probe;
    private final MeterRegistry meterRegistry;

    @Value("${chaosforge.steady-state.enabled:true}")
    private boolean enabled;

    @Value("${chaosforge.steady-state.max-consecutive-failures:3}")
    private int maxConsecutiveFailures;

    @Value("${chaosforge.steady-state.health-path:/health}")
    private String healthPath;

    // Spacing between probes within a single boundary evaluation (ADR-0533). breach window ≈
    // (maxConsecutiveFailures - 1) * probeIntervalMs; keep it well below the aggregate step deadline.
    @Value("${chaosforge.steady-state.probe-interval-ms:1000}")
    private long probeIntervalMs;

    public SteadyStateGuard(TargetHealthProbe probe, MeterRegistry meterRegistry) {
        this.probe = probe;
        this.meterRegistry = meterRegistry;
    }

    /** A fresh, stateful check for one run. Inert if disabled or no target health URL can be derived. */
    public SteadyStateCheck checkFor(RuleSetRef rules) {
        List<String> healthUrls = enabled ? deriveHealthUrls(rules) : List.of();
        return new SteadyStateCheck(healthUrls, probe, maxConsecutiveFailures, probeIntervalMs,
                () -> meterRegistry.counter("chaosforge.steady_state.breach_total").increment(),
                Thread::sleep);
    }

    /** Distinct {@code <scheme>://<authority>} + health-path across all steps (dedup, order-preserving). */
    private List<String> deriveHealthUrls(RuleSetRef rules) {
        Set<String> urls = new LinkedHashSet<>();
        for (StepSpec step : rules.steps()) {
            String healthUrl = healthUrlFor(step.targetUrl());
            if (healthUrl != null) {
                urls.add(healthUrl);
            }
        }
        return List.copyOf(urls);
    }

    private @Nullable String healthUrlFor(@Nullable String targetUrl) {
        if (targetUrl == null || targetUrl.isBlank()) {
            return null;
        }
        try {
            URI target = URI.create(targetUrl);
            if (target.getScheme() == null || target.getAuthority() == null) {
                return null;   // relative / unparseable target — cannot probe
            }
            return target.getScheme() + "://" + target.getAuthority() + healthPath;
        } catch (RuntimeException e) {
            return null;
        }
    }
}
