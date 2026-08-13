package io.chaosforge.execution.persistence;

import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** Durable fencing state (ADR-0523 Phase 1). Row-locked for the short claim tx only. */
@Component
public class InboxFenceDao {

    private final JdbcTemplate jdbc;

    public InboxFenceDao(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Ensures the fence row exists and locks it ({@code FOR UPDATE}), returning the current max_seen
     * (-1 if never seen). Must run inside the caller's transaction.
     */
    public long lockAndGetMaxSeen(UUID scenarioId, UUID tenantId) {
        jdbc.update("INSERT INTO inbox_fence (scenario_id, tenant_id, max_seen) VALUES (?, ?, -1) "
                + "ON CONFLICT (scenario_id) DO NOTHING", scenarioId, tenantId);
        Long maxSeen = jdbc.queryForObject(
                "SELECT max_seen FROM inbox_fence WHERE scenario_id = ? FOR UPDATE", Long.class, scenarioId);
        return maxSeen == null ? -1L : maxSeen;
    }

    public void advanceMaxSeen(UUID scenarioId, long version) {
        jdbc.update("UPDATE inbox_fence SET max_seen = GREATEST(max_seen, ?), updated_at = now() "
                + "WHERE scenario_id = ?", version, scenarioId);
    }
}
