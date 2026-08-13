package io.chaosforge.gateway.cache;

import java.util.UUID;

/** Per-tenant policy cached at L1 (Caffeine). Backs the rate-limit decision. */
public record TenantPolicy(UUID tenantId, int rateLimitPerMin) {

    // Fail-open fallback used ONLY when the CP policy feed is unreachable (ControlPlaneClient). Kept equal
    // to the CP's own new-tenant default (control-plane TenantController.DEFAULT_RATE_LIMIT_PER_MIN) so a
    // fallback matches what CP would have returned; the two are intentionally independent constants (no
    // cross-module dependency for a degraded-mode default) — keep them in sync (arch-audit L3).
    private static final int DEFAULT_RATE_LIMIT_PER_MIN = 600;

    public static TenantPolicy defaultFor(UUID tenantId) {
        return new TenantPolicy(tenantId, DEFAULT_RATE_LIMIT_PER_MIN);
    }
}
