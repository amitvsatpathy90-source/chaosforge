package io.chaosforge.controlplane.repository;

import io.chaosforge.controlplane.domain.OutboxMessage;
import java.util.UUID;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;
import org.jspecify.annotations.Nullable;

public interface OutboxRepository extends Repository<OutboxMessage, UUID> {

    @Modifying
    @Query("INSERT INTO outbox (message_id, aggregate_id, tenant_id, topic, partition_key, replay_version, "
            + "rule_set_id, rule_set_version, payload, tenant_signature, headers) "
            + "VALUES (:messageId, :aggregateId, :tenantId, :topic, :partitionKey, :replayVersion, "
            + ":ruleSetId, :ruleSetVersion, :payload, :tenantSignature, CAST(:headers AS jsonb))")
    void insert(@Param("messageId") UUID messageId, @Param("aggregateId") UUID aggregateId,
                @Param("tenantId") UUID tenantId, @Param("topic") String topic,
                @Param("partitionKey") String partitionKey, @Param("replayVersion") long replayVersion,
                @Param("ruleSetId") UUID ruleSetId, @Param("ruleSetVersion") int ruleSetVersion,
                @Param("payload") byte[] payload, @Param("tenantSignature") byte @Nullable [] tenantSignature,
                @Param("headers") String headers);
}
