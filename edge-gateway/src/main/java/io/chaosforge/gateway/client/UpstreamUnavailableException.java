package io.chaosforge.gateway.client;

/**
 * Control Plane returned 5xx. Recorded as a circuit-breaker fault (see resilience4j config). A 409
 * is deliberately NOT mapped here — it is a normal optimistic-concurrency response carrying
 * {@code Retry-After: 2} and must pass through as non-fault (architecture specifications: CB treats 409 as non-fault).
 */
public class UpstreamUnavailableException extends RuntimeException {
    public UpstreamUnavailableException(int status) {
        super("control plane returned " + status);
    }
}
