package io.chaosforge.controlplane.service;

import com.github.f4b6a3.uuid.UuidCreator;
import io.chaosforge.controlplane.cache.CacheInvalidationPublisher;
import io.chaosforge.controlplane.cache.TwoLevelCache;
import io.chaosforge.controlplane.domain.Tenant;
import io.chaosforge.controlplane.error.ResourceNotFoundException;
import io.chaosforge.controlplane.repository.TenantRepository;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class TenantService {

    private final TenantRepository repository;
    private final TwoLevelCache<Tenant> tenantCache;
    private final CacheInvalidationPublisher invalidation;

    public TenantService(TenantRepository repository, TwoLevelCache<Tenant> tenantCache,
                         CacheInvalidationPublisher invalidation) {
        this.repository = repository;
        this.tenantCache = tenantCache;
        this.invalidation = invalidation;
    }

    public Tenant create(String name, int rateLimitPerMin) {
        UUID tenantId = UuidCreator.getTimeOrderedEpoch();
        repository.insert(tenantId, name, "ACTIVE", rateLimitPerMin);
        return repository.findByTenantId(tenantId).orElseThrow();
    }

    public Tenant get(UUID tenantId) {
        return tenantCache.get(tenantId.toString(),
                k -> repository.findByTenantId(tenantId).orElseThrow(() -> new ResourceNotFoundException(tenantId)));
    }

    /** Mutates the tenant, then invalidates both cache tiers locally and notifies peer instances. */
    public Tenant updateRateLimit(UUID tenantId, int rateLimitPerMin) {
        if (repository.updateRateLimit(tenantId, rateLimitPerMin) == 0) {
            throw new ResourceNotFoundException(tenantId);
        }
        tenantCache.evict(tenantId.toString());                 // local L1 + L2
        invalidation.publish("tenants", tenantId.toString());   // peer L1
        return get(tenantId);
    }
}
