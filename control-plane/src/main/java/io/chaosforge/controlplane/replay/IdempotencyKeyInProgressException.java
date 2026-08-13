package io.chaosforge.controlplane.replay;

import java.util.UUID;

/**
 * The {@code (tenant_id, idempotency_key)} claim exists with status {@code IN_PROGRESS}: a concurrent
 * duplicate POST is still running its critical section. Mapped to {@code HTTP 409} with a jittered
 * {@code Retry-After} (ADR-0528). The key is not echoed — the client already holds it.
 */
public class IdempotencyKeyInProgressException extends RuntimeException {
    public IdempotencyKeyInProgressException(UUID tenantId, UUID idempotencyKey) {
        super("idempotency key in progress for tenant " + tenantId);
    }
}
