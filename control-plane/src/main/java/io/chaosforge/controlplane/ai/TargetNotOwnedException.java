package io.chaosforge.controlplane.ai;

/**
 * A draft target URL failed the ownership / SSRF gate (ai-rules.md — "the real security control").
 * Mapped to {@code HTTP 422}. The {@code reason} is a violation-shape token (e.g. {@code
 * internal_host_blocked}) — never the offending URL value (PII rule).
 */
public class TargetNotOwnedException extends RuntimeException {
    public TargetNotOwnedException(String reason) {
        super("target rejected: " + reason);
    }
}
