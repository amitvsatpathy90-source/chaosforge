package io.chaosforge.controlplane.web;

import io.chaosforge.controlplane.domain.Tenant;
import io.chaosforge.controlplane.service.TenantService;
import java.util.UUID;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Intra-service endpoint for the Edge Gateway's rate-limit policy loader. The gateway's L1 cache
 * cannot carry a user JWT (cache refresh runs outside any request context), so — like the exec
 * rule-set read — this path authenticates the <b>peer process</b>, not a tenant: under the {@code mtls}
 * profile it is restricted to the gateway's client-cert subject ({@code InternalMtlsSecurityConfig});
 * in dev/tests it rides the {@code /internal} {@code permitAll} posture.
 *
 * <p>Returns a minimal projection — only what the rate limiter needs, never the full tenant record
 * (least disclosure on a peer-asserted path). Unknown tenant → 404; the gateway fails open to its
 * default policy and counts the fallback.
 */
@RestController
@RequestMapping("/internal/tenants")
public class InternalTenantPolicyController {

    private final TenantService tenantService;

    public InternalTenantPolicyController(TenantService tenantService) {
        this.tenantService = tenantService;
    }

    @GetMapping("/{tenantId}/policy")
    public TenantPolicyResponse policy(@PathVariable UUID tenantId) {
        Tenant tenant = tenantService.get(tenantId);   // two-level cache; ResourceNotFound → 404
        return new TenantPolicyResponse(tenant.tenantId(), tenant.rateLimitPerMin());
    }

    /** Shape mirrors the gateway's {@code TenantPolicy} record — keep the two in lockstep. */
    public record TenantPolicyResponse(UUID tenantId, int rateLimitPerMin) {}
}
