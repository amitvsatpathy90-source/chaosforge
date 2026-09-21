package io.chaosforge.controlplane.ai;

/**
 * A draft target URL failed the ownership / SSRF validation gate (ADR-0534).
 *
 * <p>Mapped to {@code HTTP 422}. The {@code reason} is a violation-shape token
 * (e.g. {@code internal_host_blocked}) — never the offending URL value.
 */
public class TargetNotOwnedException extends RuntimeException {
    public TargetNotOwnedException(String reason) {
        super("target rejected: " + reason);
    }
}
