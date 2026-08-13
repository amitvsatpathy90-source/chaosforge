package io.chaosforge.controlplane.cache;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes cache-invalidation events to a Redis channel so every CP instance evicts its L1 (ADR-0504
 * explicit invalidation). The mutating instance also evicts locally; this notifies the peers. Best
 * effort — if the bus is down, peers fall back to the L2 TTL (documented staleness). The failure is
 * fail-open but not silent: it surfaces on {@code cache.l2.errors{op=invalidate_publish}}.
 */
@Component
public class CacheInvalidationPublisher {

    private static final Logger log = LoggerFactory.getLogger(CacheInvalidationPublisher.class);

    static final String CHANNEL = "chaosforge.cache.invalidation";
    static final String SEPARATOR = "::";

    private final StringRedisTemplate redis;
    private final MeterRegistry registry;

    public CacheInvalidationPublisher(StringRedisTemplate redis, MeterRegistry registry) {
        this.redis = redis;
        this.registry = registry;
    }

    public void publish(String cacheName, String key) {
        try {
            redis.convertAndSend(CHANNEL, cacheName + SEPARATOR + key);
        } catch (RuntimeException e) {
            // Bus down — peers will expire by L2 TTL; the local mutator already evicted both tiers.
            // convertAndSend ships a String, so the cause is always transport (io), never serde.
            registry.counter("cache.l2.errors", "cache", cacheName, "op", "invalidate_publish", "reason", "io")
                    .increment();
            log.debug("cache invalidation bus degraded (fail-open): cache={} type={}",
                    cacheName, e.getClass().getSimpleName());
        }
    }
}
