package io.chaosforge.controlplane.ai;

import io.chaosforge.common.target.TargetNotAllowedException;
import io.chaosforge.common.target.TargetUrlGuard;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Validates target URLs proposed by the LLM before they can be used for a tenant.
 *
 * <p>The LLM output is untrusted, so this path delegates to the shared {@link TargetUrlGuard}
 * so the AI path uses the same SSRF policy implementation as rule-set authoring and execution
 * (ADR-0534).
 *
 * <p>The AI path always enables private-network blocking, independent of the deployment-wide
 * {@code chaosforge.target.*} setting. When {@code chaosforge.ai.allowed-target-hosts} is non-empty,
 * that allowlist is the effective target ceiling and may explicitly permit a private host.
 *
 * <p><b>Deferred:</b> per-tenant target ownership backed by a future {@code tenant_targets} table.
 * Until that table exists, {@code tenantId} is retained as the seam for the future tenant-specific
 * ownership check; it is not used in the current validation (ADR-0534).
 *
 * <p>Validation failures expose only a shape token through
 * {@link TargetNotAllowedException#reason()}, never the URL value.
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
