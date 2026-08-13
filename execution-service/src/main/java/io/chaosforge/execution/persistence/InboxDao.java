package io.chaosforge.execution.persistence;

import com.github.f4b6a3.uuid.util.UuidUtil;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Message-level dedup (ADR-0523). ON CONFLICT DO NOTHING; catches Kafka redelivery of the same row.
 *
 * <p>The inbox is range-partitioned by {@code msg_ts} for O(1) partition-drop purge (C28). The
 * partition key MUST be identical across redeliveries of the same message, or a redelivery would land
 * in a different partition and escape dedup. {@code msg_ts} is therefore the timestamp embedded in the
 * UUIDv7 {@code message_id} ({@link UuidUtil#getInstant}) — a pure function of {@code message_id}, so
 * the same id always maps to the same partition and the composite unique key {@code (msg_ts,
 * message_id)} is exactly {@code message_id} dedup. ({@code message_id} is always a UUIDv7 by
 * construction — CP/Exec mint it with {@code UuidCreator.getTimeOrderedEpoch()}.)
 */
@Component
public class InboxDao {

    private final JdbcTemplate jdbc;

    public InboxDao(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** @return true if newly inserted (first sight); false if already processed (duplicate). */
    public boolean insertIfAbsent(UUID messageId, UUID tenantId, UUID scenarioId) {
        Instant msgTs = UuidUtil.getInstant(messageId);   // embedded UUIDv7 birth time — deterministic
        int rows = jdbc.update(
                "INSERT INTO inbox (message_id, msg_ts, tenant_id, scenario_id) VALUES (?, ?, ?, ?) "
                        + "ON CONFLICT (msg_ts, message_id) DO NOTHING",
                messageId, Timestamp.from(msgTs), tenantId, scenarioId);
        return rows == 1;
    }
}
