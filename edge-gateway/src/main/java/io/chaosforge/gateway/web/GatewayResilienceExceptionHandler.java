package io.chaosforge.gateway.web;

import io.chaosforge.gateway.client.UpstreamUnavailableException;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import java.util.concurrent.TimeoutException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Maps upstream-resilience failures on the gateway→CP path to honest status codes (instead of an
 * opaque 500). The CP's own responses — including its 409 + {@code Retry-After} optimistic-concurrency
 * signal — pass through {@code ControlPlaneClient} unchanged and never reach here.
 *
 * <ul>
 *   <li>{@link UpstreamUnavailableException} (CP 5xx), {@link CallNotPermittedException} (CB open),
 *       {@link BulkheadFullException} (concurrency shed) → <b>503</b> + {@code Retry-After}.</li>
 *   <li>{@link TimeoutException} (3s gateway-cp timeout) → <b>504</b> + {@code Retry-After}.</li>
 * </ul>
 * Bodies are generic — no upstream detail or tenant data leaked.
 */
@RestControllerAdvice
public class GatewayResilienceExceptionHandler {

    private static final String RETRY_AFTER_SECONDS = "2";

    @ExceptionHandler({UpstreamUnavailableException.class, CallNotPermittedException.class,
            BulkheadFullException.class})
    public ResponseEntity<ProblemDetail> unavailable(RuntimeException e) {
        return retryable(HttpStatus.SERVICE_UNAVAILABLE, "control plane unavailable");
    }

    @ExceptionHandler(TimeoutException.class)
    public ResponseEntity<ProblemDetail> timeout(TimeoutException e) {
        return retryable(HttpStatus.GATEWAY_TIMEOUT, "control plane timed out");
    }

    private static ResponseEntity<ProblemDetail> retryable(HttpStatus status, String detail) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(status, detail);
        return ResponseEntity.status(status)
                .header(HttpHeaders.RETRY_AFTER, RETRY_AFTER_SECONDS)
                .body(pd);
    }
}
