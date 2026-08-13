package io.chaosforge.controlplane.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * Two-level cache (ADR-0504): L1 Caffeine (per-instance) over L2 Redis (shared). {@code get} is
 * single-flight per key — Caffeine's atomic {@code get(key, fn)} means 100 concurrent cold reads
 * trigger exactly one L2/source load. Worst-case staleness when the invalidation bus is down equals
 * the L2 TTL. Redis failures degrade to L1 + source (fail-open), never breaking the read.
 *
 * <p><b>Tenant isolation:</b> callers must encode the tenant into the key for tenant-scoped entities
 * (e.g. {@code tenantId:scenarioId}); a scenario keyed by id alone would leak across tenants.
 */
public class TwoLevelCache<V> {

    private static final Logger log = LoggerFactory.getLogger(TwoLevelCache.class);

    private final String name;
    private final Class<V> type;
    private final Cache<String, V> l1;
    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;
    private final Duration l2Ttl;
    private final MeterRegistry registry;

    public TwoLevelCache(String name, Class<V> type, Duration l1Ttl, Duration l2Ttl,
                         StringRedisTemplate redis, ObjectMapper mapper, MeterRegistry registry) {
        this.name = name;
        this.type = type;
        this.l1 = Caffeine.newBuilder().maximumSize(10_000).expireAfterWrite(l1Ttl).build();
        this.redis = redis;
        this.mapper = mapper;
        this.l2Ttl = l2Ttl;
        this.registry = registry;
    }

    public V get(String key, Function<String, V> loader) {
        return l1.get(key, k -> throughL2(k, loader));   // single-flight per key
    }

    private V throughL2(String key, Function<String, V> loader) {
        V fromL2 = readL2(key);
        if (fromL2 != null) {
            return fromL2;
        }
        V loaded = loader.apply(key);   // source load (DB) — only when both tiers miss
        writeL2(key, loaded);
        return loaded;
    }

    /** Invalidation on mutation: clear both tiers locally. */
    public void evict(String key) {
        l1.invalidate(key);
        try {
            redis.delete(redisKey(key));
        } catch (RuntimeException e) {
            // Redis down — L1 evicted; L2 will expire by TTL. Fail-open, but no longer silent.
            recordL2Error("evict", e);
        }
    }

    /** From the invalidation listener on peer instances: clear L1 only (L2 already cleared by the mutator). */
    public void evictLocal(String key) {
        l1.invalidate(key);
    }

    public String name() {
        return name;
    }

    private @Nullable V readL2(String key) {
        try {
            String json = redis.opsForValue().get(redisKey(key));
            return json == null ? null : mapper.readValue(json, type);
        } catch (RuntimeException e) {
            // JacksonException (Jackson 3, extends RuntimeException — ADR-0520) or Redis down → miss.
            recordL2Error("read", e);
            return null;
        }
    }

    private void writeL2(String key, @Nullable V value) {
        if (value == null) {
            return;
        }
        try {
            redis.opsForValue().set(redisKey(key), mapper.writeValueAsString(value), l2Ttl);
        } catch (RuntimeException e) {
            // Non-serializable (JacksonException) or Redis down — skip L2; L1 + source still serve it.
            recordL2Error("write", e);
        }
    }

    /** Fail-open but not silent: records op/reason only — never the exception message (PII). */
    private void recordL2Error(String op, RuntimeException e) {
        boolean serde = e instanceof JacksonException;
        String reason = serde ? "serde" : "io";
        registry.counter("cache.l2.errors", "cache", name, "op", op, "reason", reason).increment();
        log.debug("L2 cache degraded (fail-open): op={} cache={} reason={} type={}",
                op, name, reason, e.getClass().getSimpleName());
    }

    private String redisKey(String key) {
        return "cache:" + name + ":" + key;
    }
}
