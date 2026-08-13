package io.chaosforge.execution.steadystate;

import io.chaosforge.common.target.TargetUrlGuard;
import java.net.URI;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * Real target-health probe (acceptance gate C20) — a GET to the target's health endpoint over the
 * dedicated short-timeout {@code probeRestClient} (arch-audit M1), NOT the 30s step client: a health
 * check must resolve fast because the probe loop runs inside the aggregate step deadline. {@code exchange}
 * reads the raw status without the default 4xx/5xx throw; any non-2xx — or a refused connection / timeout
 * / DNS failure — counts as unhealthy.
 *
 * <p>The probe URL is run through the same {@link TargetUrlGuard} as the step-execution path
 * ({@code CpRuleSetLoader}), so the SSRF / blast-radius boundary is identical no matter which code
 * path reaches out to a target. A guard-rejected URL throws {@code TargetNotAllowedException} (a
 * {@code RuntimeException}) and is caught below as "not healthy" — the fail-safe outcome, since a
 * target whose health cannot be legitimately checked should not be under active fault injection.
 */
@Component
public class HttpTargetHealthProbe implements TargetHealthProbe {

    private final RestClient probe;
    private final TargetUrlGuard guard;

    public HttpTargetHealthProbe(RestClient probeRestClient, TargetUrlGuard guard) {
        this.probe = probeRestClient;
        this.guard = guard;
    }

    @Override
    public boolean isHealthy(String healthUrl) {
        try {
            guard.validate(healthUrl);   // same SSRF/blast-radius gate as the step path (ADR-0534)
            HttpStatusCode status = probe.get()
                    .uri(URI.create(healthUrl))
                    .exchange((request, response) -> response.getStatusCode());
            return status.is2xxSuccessful();
        } catch (RuntimeException e) {
            return false;   // blocked target / unreachable / timed out / malformed → not healthy
        }
    }
}
