package io.chaosforge.gateway.cache;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.chaosforge.gateway.client.ControlPlaneClient;
import java.time.Duration;
import java.util.UUID;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * L1-only tenant-policy cache (Caffeine, ADR-0504). No Redis L2 in the gateway. The loader returns a
 * {@link java.util.concurrent.CompletableFuture} via {@code WebClient} → CP — it never blocks.
 */
@Component
public class TenantPolicyCache {

    private final AsyncLoadingCache<UUID, TenantPolicy> cache;

    public TenantPolicyCache(ControlPlaneClient controlPlaneClient) {
        this.cache = Caffeine.newBuilder()
                .maximumSize(10_000)
                .expireAfterWrite(Duration.ofMinutes(5))
                .refreshAfterWrite(Duration.ofMinutes(4))
                .buildAsync((tenantId, executor) -> controlPlaneClient.fetchTenantPolicy(tenantId).toFuture());
    }

    public Mono<TenantPolicy> get(UUID tenantId) {
        return Mono.fromFuture(cache.get(tenantId));
    }
}
