package io.chaosforge.execution.step;

import io.chaosforge.execution.dlq.DlqRoutableException;
import io.chaosforge.execution.ruleset.StepSpec;
import io.github.resilience4j.bulkhead.annotation.Bulkhead;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import java.util.Locale;
import java.util.UUID;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * Executes one step against its target, Resilience4j-wrapped ({@code target-call}). The
 * Idempotency-Key {@code scenario_id:replay_version:step_id} is stable across retries (never
 * regenerated). A malformed step → STEP_FAILED (scenario logic error). Infra failures (CB open /
 * target unreachable) surface as the thrown exception, translated to INFRA_TRANSIENT by the caller.
 */
@Component
public class StepExecutor {

    private final RestClient target;

    public StepExecutor(RestClient targetRestClient) {
        this.target = targetRestClient;
    }

    /** @return the target's HTTP status code. */
    @CircuitBreaker(name = "target-call")
    @Bulkhead(name = "target-call")
    public int execute(UUID scenarioId, long replayVersion, StepSpec step) {
        if (step.targetUrl() == null || step.targetUrl().isBlank()) {
            throw DlqRoutableException.stepFailed("step " + step.stepId() + " has no targetUrl");
        }
        HttpMethod method;
        try {
            method = HttpMethod.valueOf((step.method() == null ? "GET" : step.method()).toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw DlqRoutableException.stepFailed("step " + step.stepId() + " has invalid method " + step.method());
        }
        String idempotencyKey = scenarioId + ":" + replayVersion + ":" + step.stepId();
        return target.method(method)
                .uri(step.targetUrl())
                .header("Idempotency-Key", idempotencyKey)
                .exchange((request, response) -> response.getStatusCode().value());   // no default 4xx/5xx throw
    }
}
