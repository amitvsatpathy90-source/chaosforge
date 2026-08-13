package io.chaosforge.controlplane.repository;

import io.chaosforge.controlplane.domain.Scenario;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

/**
 * Tenant-scoped scenario access. Every query carries {@code tenantId} (ADR-0509). There is
 * deliberately NO {@code findById(UUID)} — a cross-tenant lookup returns empty → 404 (ADR-0510).
 */
public interface ScenarioRepository extends Repository<Scenario, UUID> {

    @Modifying
    @Query("INSERT INTO scenarios (scenario_id, tenant_id, name, rule_set_id, rule_set_version) "
            + "VALUES (:scenarioId, :tenantId, :name, :ruleSetId, :ruleSetVersion)")
    void insert(@Param("scenarioId") UUID scenarioId, @Param("tenantId") UUID tenantId,
                @Param("name") String name, @Param("ruleSetId") UUID ruleSetId,
                @Param("ruleSetVersion") int ruleSetVersion);

    @Query("SELECT scenario_id, tenant_id, name, rule_set_id, rule_set_version, status, created_at, updated_at "
            + "FROM scenarios WHERE scenario_id = :scenarioId AND tenant_id = :tenantId")
    Optional<Scenario> findByScenarioIdAndTenantId(
            @Param("scenarioId") UUID scenarioId, @Param("tenantId") UUID tenantId);

    @Query("SELECT scenario_id, tenant_id, name, rule_set_id, rule_set_version, status, created_at, updated_at "
            + "FROM scenarios WHERE tenant_id = :tenantId ORDER BY created_at DESC")
    List<Scenario> findAllByTenantId(@Param("tenantId") UUID tenantId);

    /** Tenant-scoped read of the current fencing token (replay_version) for the ETag on GET. */
    @Query("SELECT st.replay_version FROM scenario_replay_state st "
            + "JOIN scenarios s ON s.scenario_id = st.scenario_id "
            + "WHERE st.scenario_id = :scenarioId AND s.tenant_id = :tenantId")
    Optional<Long> findReplayVersion(@Param("scenarioId") UUID scenarioId, @Param("tenantId") UUID tenantId);
}
