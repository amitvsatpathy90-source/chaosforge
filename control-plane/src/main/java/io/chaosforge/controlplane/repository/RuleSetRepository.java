package io.chaosforge.controlplane.repository;

import io.chaosforge.controlplane.domain.RuleSet;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

/** Tenant-scoped, append-only rule sets (ADR-0503). No {@code findById(UUID)}. */
public interface RuleSetRepository extends Repository<RuleSet, UUID> {

    @Modifying
    @Query("INSERT INTO rule_sets (rule_set_id, version, tenant_id, name, definition) "
            + "VALUES (:ruleSetId, :version, :tenantId, :name, CAST(:definition AS jsonb))")
    void insert(@Param("ruleSetId") UUID ruleSetId, @Param("version") int version,
                @Param("tenantId") UUID tenantId, @Param("name") String name,
                @Param("definition") String definition);

    @Query("SELECT rule_set_id, version, tenant_id, name, definition::text AS definition, created_at "
            + "FROM rule_sets WHERE rule_set_id = :ruleSetId AND version = :version AND tenant_id = :tenantId")
    Optional<RuleSet> findByRuleSetIdAndVersionAndTenantId(
            @Param("ruleSetId") UUID ruleSetId, @Param("version") int version, @Param("tenantId") UUID tenantId);

    @Query("SELECT coalesce(max(version), 0) FROM rule_sets WHERE rule_set_id = :ruleSetId AND tenant_id = :tenantId")
    int currentMaxVersion(@Param("ruleSetId") UUID ruleSetId, @Param("tenantId") UUID tenantId);
}
