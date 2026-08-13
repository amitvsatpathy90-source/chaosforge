package io.chaosforge.controlplane.domain;

import java.time.Instant;
import java.util.UUID;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

/**
 * CP transactional-outbox row. Written via an explicit {@code @Modifying} insert (schema-at-write,
 * Avro payload — ADR-0525); reads are exercised by the OutboxPoller in the outbox-relay step.
 */
@Table("outbox")
public record OutboxMessage(
        @Id UUID messageId,
        String aggregateType,
        UUID aggregateId,
        UUID tenantId,
        String topic,
        String partitionKey,
        long replayVersion,
        UUID ruleSetId,
        int ruleSetVersion,
        byte[] payload,
        byte[] tenantSignature,
        String headers,        // jsonb
        String status,
        int attempts,
        String lastError,
        Instant createdAt,
        Instant sentAt) {
}
