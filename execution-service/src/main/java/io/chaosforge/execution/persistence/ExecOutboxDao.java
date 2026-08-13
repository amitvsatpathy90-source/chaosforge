package io.chaosforge.execution.persistence;

import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** Result-event outbox (ADR-0523 Phase 3). Shipped to Kafka by the exec outbox relay (mirrors CP). */
@Component
public class ExecOutboxDao {

    private final JdbcTemplate jdbc;

    public ExecOutboxDao(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insertResultEvent(UUID messageId, UUID scenarioId, UUID tenantId, long replayVersion,
                                  String topic, String partitionKey, byte[] payload, String headers) {
        jdbc.update("INSERT INTO outbox (message_id, aggregate_id, tenant_id, topic, partition_key, "
                + "replay_version, payload, headers) VALUES (?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb))",
                messageId, scenarioId, tenantId, topic, partitionKey, replayVersion, payload, headers);
    }
}
