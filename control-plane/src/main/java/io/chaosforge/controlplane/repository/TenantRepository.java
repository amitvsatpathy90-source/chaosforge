package io.chaosforge.controlplane.repository;

import io.chaosforge.controlplane.domain.Tenant;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

/**
 * Extends the bare {@link Repository} — NOT CrudRepository/JpaRepository — so no {@code findById(UUID)}
 * is inherited (ADR-0509, ArchUnit-enforced). Tenants are the isolation root.
 */
public interface TenantRepository extends Repository<Tenant, UUID> {

    @Modifying
    @Query("INSERT INTO tenants (tenant_id, name, status, rate_limit_per_min) "
            + "VALUES (:tenantId, :name, :status, :rateLimitPerMin)")
    void insert(@Param("tenantId") UUID tenantId, @Param("name") String name,
                @Param("status") String status, @Param("rateLimitPerMin") int rateLimitPerMin);

    @Query("SELECT tenant_id, name, status, rate_limit_per_min, created_at, updated_at "
            + "FROM tenants WHERE tenant_id = :tenantId")
    Optional<Tenant> findByTenantId(@Param("tenantId") UUID tenantId);

    @Modifying
    @Query("UPDATE tenants SET rate_limit_per_min = :rateLimitPerMin WHERE tenant_id = :tenantId")
    int updateRateLimit(@Param("tenantId") UUID tenantId, @Param("rateLimitPerMin") int rateLimitPerMin);
}
