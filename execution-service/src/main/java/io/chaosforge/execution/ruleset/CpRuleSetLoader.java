package io.chaosforge.execution.ruleset;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.chaosforge.common.target.TargetNotAllowedException;
import io.chaosforge.common.target.TargetUrlGuard;
import io.chaosforge.execution.dlq.DlqRoutableException;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * Loads the pinned rule set from CP over (m)TLS and caches it. Append-only, so never invalidated,
 * no TTL (ADR-0503). CB + bulkhead composed programmatically (AOP self-invocation would be
 * bypassed inside the Caffeine loader lambda); a cold-key failure maps to INFRA_TRANSIENT.
 */
@Component
public class CpRuleSetLoader {

    private final RestClient controlPlane;
    private final CircuitBreaker circuitBreaker;
    private final Bulkhead bulkhead;
    private final TargetUrlGuard targetUrlGuard;
    private final Cache<String, RuleSetRef> cache = Caffeine.newBuilder().maximumSize(10_000).build();

    public CpRuleSetLoader(RestClient controlPlaneRestClient, CircuitBreakerRegistry cbRegistry,
                           BulkheadRegistry bulkheadRegistry, TargetUrlGuard targetUrlGuard) {
        this.controlPlane = controlPlaneRestClient;
        this.circuitBreaker = cbRegistry.circuitBreaker("control-plane-call");
        this.bulkhead = bulkheadRegistry.bulkhead("control-plane-call");
        this.targetUrlGuard = targetUrlGuard;
    }

    public RuleSetRef loadPinned(UUID ruleSetId, int version, UUID tenantId) {
        return cache.get("rule_set:" + ruleSetId + ":" + version, key -> {
            RuleSetRef ref = fetch(ruleSetId, version, tenantId);
            // SSRF guard (arch-audit HIGH-2): validated once on cache load, outside the CB-wrapped
            // fetch, so a blocked host is terminal STEP_FAILED, never replayable INFRA_TRANSIENT.
            guardTargets(ref);
            return ref;
        });
    }

    /** Every distinct step target must pass the guard, else the whole run is terminal STEP_FAILED.
     *  Null/blank targets are left to {@code StepExecutor}'s own STEP_FAILED check. */
    private void guardTargets(RuleSetRef ref) {
        for (StepSpec step : ref.steps()) {
            if (step.targetUrl() == null || step.targetUrl().isBlank()) {
                continue;
            }
            try {
                targetUrlGuard.validate(step.targetUrl());
            } catch (TargetNotAllowedException e) {
                throw DlqRoutableException.stepFailed(
                        "step " + step.stepId() + " target rejected: " + e.reason());
            }
        }
    }

    private RuleSetRef fetch(UUID ruleSetId, int version, UUID tenantId) {
        try {
            // bulkhead outermost (shed load) → CB → call.
            return bulkhead.executeSupplier(() ->
                    circuitBreaker.executeSupplier(() -> callControlPlane(ruleSetId, version, tenantId)));
        } catch (RuntimeException e) {
            // Includes CallNotPermittedException (CB open) and BulkheadFullException (shed) → replayable.
            throw DlqRoutableException.infraTransient("rule-set load failed for " + ruleSetId + ":" + version, e);
        }
    }

    private RuleSetRef callControlPlane(UUID ruleSetId, int version, UUID tenantId) {
        StepSpec[] steps = controlPlane.get()
                .uri(uri -> uri.path("/internal/rule-sets/{id}/versions/{v}")
                        .queryParam("tenantId", tenantId).build(ruleSetId, version))
                .retrieve()
                .body(StepSpec[].class);
        return new RuleSetRef(ruleSetId, version, steps == null ? List.of() : List.of(steps));
    }
}
