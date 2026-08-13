package io.chaosforge.controlplane.ai;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.util.Set;
import java.util.UUID;
import org.springframework.stereotype.Service;

/**
 * Tier-1 AI authoring (ADR-0518). Orchestrates the mandatory post-draft sequence:
 * <ol>
 *   <li>call the model boundary ({@link OllamaAuthoringClient}, CB-wrapped);</li>
 *   <li>Bean Validation schema gate → {@link AiOutputValidationException} (422);</li>
 *   <li>tenant target-URL ownership / SSRF gate → {@link TargetNotOwnedException} (422);</li>
 *   <li>return the draft for human review — <b>never auto-persist</b> (the client commits with a
 *       second explicit request).</li>
 * </ol>
 * Validation runs <b>outside</b> the model's circuit breaker, so a bad draft is a 422 and never trips
 * the breaker or routes to the AI-unavailable fallback. This stays entirely off the deterministic
 * replay path — it is an authoring tool only.
 */
@Service
public class ScenarioAuthoringService {

    private final OllamaAuthoringClient authoringClient;
    private final Validator validator;
    private final TenantTargetValidator tenantTargetValidator;
    private final Timer latency;
    private final MeterRegistry registry;

    public ScenarioAuthoringService(OllamaAuthoringClient authoringClient, Validator validator,
                                    TenantTargetValidator tenantTargetValidator, MeterRegistry registry) {
        this.authoringClient = authoringClient;
        this.validator = validator;
        this.tenantTargetValidator = tenantTargetValidator;
        this.registry = registry;
        this.latency = registry.timer("chaosforge.ai.authoring.latency");
    }

    public ScenarioDraft generateDraft(ScenarioDraftRequest request, UUID tenantId) {
        long start = System.nanoTime();
        // (1) Model boundary — AiUnavailableException propagates from the CB fallback on failure.
        ScenarioDraft draft;
        try {
            draft = authoringClient.generate(request);
        } catch (AiUnavailableException e) {
            outcome("ai-unavailable");
            throw e;
        }
        try {
            // (2) Bean Validation schema gate.
            Set<ConstraintViolation<ScenarioDraft>> violations = validator.validate(draft);
            if (!violations.isEmpty()) {
                throw new AiOutputValidationException(violations);
            }
            // (3) Tenant target-URL ownership / SSRF gate — the real security control.
            tenantTargetValidator.assertOwned(draft.targetUrls(), tenantId);
        } catch (RuntimeException e) {
            outcome("validation-failed");
            throw e;
        }
        outcome("success");
        latency.record(System.nanoTime() - start, java.util.concurrent.TimeUnit.NANOSECONDS);
        return draft;   // (4) returned for review; NOT persisted
    }

    private void outcome(String outcome) {
        registry.counter("chaosforge.ai.authoring.requests", "outcome", outcome).increment();
    }
}
