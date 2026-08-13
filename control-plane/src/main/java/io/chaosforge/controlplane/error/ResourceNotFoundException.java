package io.chaosforge.controlplane.error;

import java.util.UUID;

/**
 * A tenant-scoped lookup returned empty — either the resource does not exist or it belongs to
 * another tenant. The two are deliberately indistinguishable: the handler maps this to {@code 404},
 * never {@code 403} (ADR-0510), so resource existence is not leaked across tenants.
 */
public class ResourceNotFoundException extends RuntimeException {
    public ResourceNotFoundException(UUID resourceId) {
        super("resource not found: " + resourceId);
    }

    /** For non-UUID-addressed resources (e.g. a DLQ record at topic/partition/offset). */
    public ResourceNotFoundException(String message) {
        super(message);
    }
}
