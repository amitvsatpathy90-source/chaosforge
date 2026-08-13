package io.chaosforge.controlplane.security;

import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Per-request tenant identity, populated by {@link JwtTenantExtractionFilter} from the VERIFIED JWT
 * claim (ADR-0524) — never from an {@code X-Tenant-Id} header. Layer 2 of the three-layer isolation
 * model (ADR-0509). Always cleared in the filter's finally block so it never leaks across requests.
 */
public final class TenantContext {

    private static final ThreadLocal<UUID> CURRENT = new ThreadLocal<>();

    private TenantContext() {}

    public static void set(UUID tenantId) {
        CURRENT.set(tenantId);
    }

    /** @return the current tenant id, or {@code null} if none is bound. */
    public static @Nullable UUID get() {
        return CURRENT.get();
    }

    /**
     * @return the current tenant id.
     * @throws TenantContextMissingException if no tenant is bound — a missing context would
     *         otherwise silently produce {@code WHERE tenant_id = NULL} (zero rows), masking the bug.
     */
    public static UUID require() {
        UUID tenantId = CURRENT.get();
        if (tenantId == null) {
            throw new TenantContextMissingException();
        }
        return tenantId;
    }

    public static void clear() {
        CURRENT.remove();
    }
}
