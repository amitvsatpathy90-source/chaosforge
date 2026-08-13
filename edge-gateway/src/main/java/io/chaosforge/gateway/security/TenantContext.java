package io.chaosforge.gateway.security;

/**
 * Reactor Context key for the tenant id. In the gateway, tenant identity flows through the
 * Reactor Context — NEVER a {@code ThreadLocal} (thread hops in a reactive chain make it invisible).
 */
public final class TenantContext {

    public static final String KEY = "chaosforge.tenant-id";

    private TenantContext() {}
}
