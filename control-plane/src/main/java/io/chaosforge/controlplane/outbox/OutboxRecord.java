package io.chaosforge.controlplane.outbox;

import java.time.OffsetDateTime;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * A claimed outbox row, projected for publishing. {@code payload} is Avro binary (ADR-0525).
 * {@code attempts} is the post-claim publish count (the claim increments it) — carried so the poller
 * can quarantine a record-fatal poison row as DEAD once it crosses {@code max-attempts} (ADR-0528).
 *
 * <p>{@code msgTs} is the partition key of the range-partitioned {@code outbox} (C28). It is carried
 * from claim to finalize so every finalize UPDATE — and the claim's own self-join — can predicate on
 * {@code (msg_ts, message_id)}, the composite PK, and prune to a single partition. Matching on
 * {@code message_id} alone would scan every live partition (arch-audit 2.5 / G1).
 *
 * <p>{@code priorClaimedBy} is the relay instance that held the previous lease on this row
 * ({@code null} on first claim). A non-null value naming a <em>different</em> instance is a lease
 * takeover — the at-least-once duplicate-publish window — surfaced as {@code outbox.lease_takeovers}.
 */
public record OutboxRecord(
        UUID messageId,
        OffsetDateTime msgTs,
        UUID tenantId,
        String topic,
        String partitionKey,
        long replayVersion,
        UUID ruleSetId,
        int ruleSetVersion,
        byte[] payload,
        int attempts,
        @Nullable String priorClaimedBy) {
}
