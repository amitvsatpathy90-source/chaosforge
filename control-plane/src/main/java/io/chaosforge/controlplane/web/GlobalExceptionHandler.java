package io.chaosforge.controlplane.web;

import io.chaosforge.controlplane.ai.AiOutputValidationException;
import io.chaosforge.controlplane.ai.AiUnavailableException;
import io.chaosforge.controlplane.ai.TargetNotOwnedException;
import io.chaosforge.controlplane.error.ResourceNotFoundException;
import io.chaosforge.controlplane.replay.ConcurrentReplayException;
import io.chaosforge.controlplane.replay.IdempotencyKeyInProgressException;
import io.chaosforge.controlplane.security.TenantContextMissingException;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Central error mapping. Messages are intentionally generic — no tenant ids, field values, or
 * resource-existence hints (PII rule; ADR-0510 404-not-403).
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(ResourceNotFoundException.class)
    public ProblemDetail notFound(ResourceNotFoundException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, "resource not found");
    }

    @ExceptionHandler(ConcurrentReplayException.class)
    public ResponseEntity<ProblemDetail> concurrentReplay(ConcurrentReplayException e) {
        return conflict("concurrent replay detected");
    }

    @ExceptionHandler(IdempotencyKeyInProgressException.class)
    public ResponseEntity<ProblemDetail> idempotencyInProgress(IdempotencyKeyInProgressException e) {
        return conflict("a request with this idempotency key is already in progress");
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail badInput(IllegalArgumentException e) {
        // e.g. ReplayCommand rejecting expectedVersion < 0. Message is safe (no PII).
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "invalid request");
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail invalid(MethodArgumentNotValidException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "validation failed");
    }

    // AI authoring (ADR-0518). Draft schema / target-ownership failures are the client's to fix (422);
    // an unavailable model is retryable (503). Messages carry violation shapes only, never values.
    @ExceptionHandler({AiOutputValidationException.class, TargetNotOwnedException.class})
    public ProblemDetail aiOutputRejected(RuntimeException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.UNPROCESSABLE_CONTENT, e.getMessage());
    }

    @ExceptionHandler(AiUnavailableException.class)
    public ResponseEntity<ProblemDetail> aiUnavailable(AiUnavailableException e) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, "5")
                .body(pd);
    }

    @ExceptionHandler(TenantContextMissingException.class)
    public ProblemDetail tenantMissing(TenantContextMissingException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR, "tenant context missing");
    }

    /**
     * 409 + jittered {@code Retry-After} (ADR-0528). Jitter (1–3 s, not a fixed value) prevents a
     * synchronized retry storm on a hot scenario.
     */
    private static ResponseEntity<ProblemDetail> conflict(String detail) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, detail);
        int retryAfterSeconds = 1 + ThreadLocalRandom.current().nextInt(3);   // {1, 2, 3}
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .header(HttpHeaders.RETRY_AFTER, Integer.toString(retryAfterSeconds))
                .body(pd);
    }
}
