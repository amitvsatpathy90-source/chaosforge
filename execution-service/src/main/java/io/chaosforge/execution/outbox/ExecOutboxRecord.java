package io.chaosforge.execution.outbox;

import java.time.OffsetDateTime;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * A claimed result-event outbox row, projected for publishing. {@code payload} is the result-event
 * body — Avro binary {@code ScenarioRunResult} (ADR-0525, encoded at insert). {@code attempts}
 * is the post-claim publish count (the claim increments it) so the poller can quarantine a
 * record-fatal poison row as DEAD once it crosses {@code max-attempts}. Mirrors the CP
 * {@code OutboxRecord} minus the rule-set tuple, which a result event does not carry.
 *
 * <p>{@code msgTs} is the partition key of the range-partitioned {@code outbox} (C28), carried from
 * claim to finalize so every finalize UPDATE — and the claim's own self-join — predicates on the full
 * {@code (msg_ts, message_id)} PK and prunes to one partition instead of scanning every live
 * partition (arch-audit 2.5 / G1).
 *
 * <p>{@code priorClaimedBy} is the relay instance that held the previous lease ({@code null} on first
 * claim); a different instance re-claiming a still-PENDING row is a lease takeover — the
 * at-least-once duplicate-publish window — surfaced as {@code outbox.lease_takeovers}.
 */
public record ExecOutboxRecord(
        UUID messageId,
        OffsetDateTime msgTs,
        UUID tenantId,
        String topic,
        String partitionKey,
        long replayVersion,
        byte[] payload,
        int attempts,
        @Nullable String priorClaimedBy) {
}
