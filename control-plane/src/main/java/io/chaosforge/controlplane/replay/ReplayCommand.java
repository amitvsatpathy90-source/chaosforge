package io.chaosforge.controlplane.replay;

import java.util.UUID;
import org.jspecify.annotations.NonNull;

/**
 * All inputs to the replay critical section (ADR-0528). Validated at construction — bad input never
 * reaches the DB.
 *
 * <p>{@code @NonNull} appears only on the reference-type fields; {@code expectedVersion} is a
 * primitive {@code long} (annotating it would be meaningless — ADR-0528 forbidden-patterns list).
 *
 * @param scenarioId     the target scenario
 * @param tenantId       authoritative tenant id from the re-verified JWT only (ADR-0524) — never the
 *                       {@code X-Tenant-Id} header
 * @param expectedVersion the client's last-observed {@code replay_version} ({@code If-Match}); the CAS
 *                       predicate. Never derived from a DB read in the same transaction.
 * @param idempotencyKey client-generated per logical attempt; the at-most-once retry guard
 */
public record ReplayCommand(
        @NonNull UUID scenarioId,
        @NonNull UUID tenantId,
        long expectedVersion,
        @NonNull UUID idempotencyKey) {

    public ReplayCommand {
        // expectedVersion < 0 is structurally impossible on a valid row; reject pre-DB → HTTP 400.
        if (expectedVersion < 0) {
            throw new IllegalArgumentException("expectedVersion must be >= 0, got: " + expectedVersion);
        }
    }
}
