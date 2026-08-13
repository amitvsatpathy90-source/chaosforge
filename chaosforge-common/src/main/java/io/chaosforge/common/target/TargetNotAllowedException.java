package io.chaosforge.common.target;

/**
 * A scenario target URL was rejected by {@link TargetUrlGuard}. The {@link #reason()} is a shape token
 * ({@code internal_host_blocked}, {@code not_in_allowlist}, …) — never the URL value (PII rule). Each
 * service maps this to its own boundary outcome: the Control Plane → HTTP 422, the Execution Service →
 * a terminal {@code STEP_FAILED} DLQ routing.
 */
public class TargetNotAllowedException extends RuntimeException {

    private final String reason;

    public TargetNotAllowedException(String reason) {
        super("target rejected: " + reason);
        this.reason = reason;
    }

    public String reason() {
        return reason;
    }
}
