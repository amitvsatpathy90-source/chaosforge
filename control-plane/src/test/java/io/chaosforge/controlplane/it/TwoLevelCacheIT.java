package io.chaosforge.controlplane.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.chaosforge.controlplane.cache.TwoLevelCache;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.json.JsonMapper;

/**
 * Two-level cache single-flight (ADR-0504) against a real Redis L2. The headline claim: N concurrent
 * cold reads of the same key trigger exactly ONE source load (Caffeine L1 {@code get(key, fn)} is
 * atomic per key), and L2 is populated for the next miss.
 */
class TwoLevelCacheIT {

    @SuppressWarnings("resource")
    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine")).withExposedPorts(6379);

    private static LettuceConnectionFactory connectionFactory;
    private static StringRedisTemplate redis;

    @BeforeAll
    static void startRedis() {
        REDIS.start();
        connectionFactory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getFirstMappedPort()));
        connectionFactory.afterPropertiesSet();
        connectionFactory.start();
        redis = new StringRedisTemplate(connectionFactory);
        redis.afterPropertiesSet();
    }

    @AfterAll
    static void stopRedis() {
        connectionFactory.destroy();
        REDIS.stop();
    }

    @Test
    void hundredConcurrentColdReads_triggerExactlyOneLoad() throws Exception {
        TwoLevelCache<CachedPolicy> cache = new TwoLevelCache<>(
                "tenant", CachedPolicy.class, Duration.ofMinutes(5), Duration.ofHours(1),
                redis, JsonMapper.builder().build(), new SimpleMeterRegistry());

        AtomicInteger loads = new AtomicInteger();
        java.util.function.Function<String, CachedPolicy> loader = key -> {
            loads.incrementAndGet();
            sleep(50);   // widen the race window so threads pile up on the same cold key
            return new CachedPolicy(key, 42);
        };

        int n = 100;
        ExecutorService pool = Executors.newFixedThreadPool(32);
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<CachedPolicy>> results = new java.util.ArrayList<>();
        for (int i = 0; i < n; i++) {
            results.add(pool.submit((Callable<CachedPolicy>) () -> {
                gate.await();
                return cache.get("policy:k", loader);
            }));
        }
        gate.countDown();   // release all 100 at once
        for (Future<CachedPolicy> f : results) {
            assertThat(f.get(15, TimeUnit.SECONDS)).isEqualTo(new CachedPolicy("policy:k", 42));
        }
        pool.shutdownNow();

        assertThat(loads.get()).as("single-flight: exactly one source load for the cold key").isEqualTo(1);
        assertThat(redis.opsForValue().get("cache:tenant:policy:k")).as("L2 populated").isNotNull();
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** A small cacheable value; Jackson 3 serializes the record to/from L2 JSON. */
    record CachedPolicy(String tenantKey, int rateLimit) {
    }
}
