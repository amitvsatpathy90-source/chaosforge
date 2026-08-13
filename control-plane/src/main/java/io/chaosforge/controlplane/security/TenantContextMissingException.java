package io.chaosforge.controlplane.security;

/** Thrown by {@link TenantContext#require()} when no tenant is bound to the current request. */
public class TenantContextMissingException extends RuntimeException {
    public TenantContextMissingException() {
        super("No tenant bound to the current request — JWT extraction filter did not run");
    }
}
