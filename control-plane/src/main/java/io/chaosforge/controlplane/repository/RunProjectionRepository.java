package io.chaosforge.controlplane.repository;

import io.chaosforge.controlplane.domain.RunProjection;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

/** Tenant-scoped run-result cache. No {@code findById(UUID)} — see ADR-0510. */
public interface RunProjectionRepository extends Repository<RunProjection, UUID> {

    // Redelivery-safe: a repeat of the same terminal event is a no-op update, not a duplicate row.
    @Modifying
    @Query("INSERT INTO run_projection (scenario_id, replay_version, tenant_id, outcome, finished_at) "
            + "VALUES (:scenarioId, :replayVersion, :tenantId, :outcome, :finishedAt) "
            + "ON CONFLICT (scenario_id, replay_version) DO UPDATE "
            + "SET outcome = EXCLUDED.outcome, finished_at = EXCLUDED.finished_at, updated_at = now()")
    void upsert(@Param("scenarioId") UUID scenarioId, @Param("replayVersion") long replayVersion,
                @Param("tenantId") UUID tenantId, @Param("outcome") String outcome,
                @Param("finishedAt") java.time.Instant finishedAt);

    @Query("SELECT scenario_id, replay_version, tenant_id, outcome, finished_at, updated_at "
            + "FROM run_projection WHERE scenario_id = :scenarioId AND replay_version = :replayVersion "
            + "AND tenant_id = :tenantId")
    Optional<RunProjection> findByScenarioIdAndReplayVersionAndTenantId(
            @Param("scenarioId") UUID scenarioId, @Param("replayVersion") long replayVersion,
            @Param("tenantId") UUID tenantId);
}
