package io.chaosforge.controlplane.ai;

import io.chaosforge.common.target.TargetNotAllowedException;
import io.chaosforge.common.target.TargetUrlGuard;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Validates that LLM-proposed target URLs are safe for a tenant to call (ai-rules.md — "the real
 * security control"; the LLM output is untrusted). Delegates to the shared {@link TargetUrlGuard} so the
 * AI path enforces the identical SSRF policy as rule-set authoring and execution (arch-audit HIGH-2).
 * The AI path <b>always</b> blocks private networks (an LLM must never be trusted to propose an internal
 * host), independent of the deployment-wide {@code chaosforge.target.*} toggle; it additionally honours
 * the {@code chaosforge.ai.allowed-target-hosts} allowlist.
 * <p><b>Deferred:</b> per-tenant target ownership backed by a {@code tenant_targets} table. There is
 * no such table yet, so {@code tenantId} is the contract seam but not yet a data-backed predicate.
 * Violation messages carry a shape token only, never the URL value (PII rule).
 */
@Component
public class TenantTargetValidator {

    private final TargetUrlGuard guard;

    public TenantTargetValidator(
            @Value("${chaosforge.ai.allowed-target-hosts:}") List<String> allowedHosts) {
        this.guard = new TargetUrlGuard(true, allowedHosts);   // AI path: always block private networks
    }

    /** @throws TargetNotOwnedException on the first URL that fails the gate. */
    public void assertOwned(List<String> targetUrls, UUID tenantId) {
        try {
            guard.validateAll(targetUrls);
        } catch (TargetNotAllowedException e) {
            throw new TargetNotOwnedException(e.reason());
        }
    }
}
