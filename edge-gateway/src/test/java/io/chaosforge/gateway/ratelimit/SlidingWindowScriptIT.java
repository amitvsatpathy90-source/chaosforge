package io.chaosforge.gateway.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * GAP-03 grounding: runs the ACTUAL {@code sliding_window.lua} against a real Redis and proves (a) it
 * enforces the per-tenant limit and (b) its window slides on the <b>Redis server clock</b> — no
 * caller-supplied timestamp is passed, so cross-pod wall-clock drift can't corrupt the one shared key.
 * A mocked {@code RedisTemplate} ({@code RateLimitWebFilterTest}) can prove neither; this is the
 * executable proof the script is syntactically valid and TIME-sourced.
 */
class SlidingWindowScriptIT {

    @SuppressWarnings("resource")
    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine")).withExposedPorts(6379);

    private static final RedisScript<Long> SCRIPT = script();
    private static LettuceConnectionFactory connectionFactory;
    private static ReactiveStringRedisTemplate redis;

    @BeforeAll
    static void start() {
        REDIS.start();
        connectionFactory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getFirstMappedPort()));
        connectionFactory.afterPropertiesSet();
        connectionFactory.start();
        redis = new ReactiveStringRedisTemplate(connectionFactory);
    }

    @AfterAll
    static void stop() {
        connectionFactory.destroy();
        REDIS.stop();
    }

    @Test
    void enforcesLimit_withNoCallerTimestamp() {
        String rateKey = "{tenant:" + UUID.randomUUID() + "}:rate";
        List<String> keys = List.of(rateKey, rateKey + ":seq");
        long window = 60_000L;
        int limit = 3;

        // Only window + limit are passed — NO now_ms. The script sources `now` from redis TIME (GAP-03).
        assertThat(run(keys, window, limit)).isEqualTo(2L);    // remaining after the 1st admit
        assertThat(run(keys, window, limit)).isEqualTo(1L);
        assertThat(run(keys, window, limit)).isEqualTo(0L);
        assertThat(run(keys, window, limit)).isEqualTo(-1L);   // breach sentinel
        assertThat(run(keys, window, limit)).isEqualTo(-1L);   // still breached
    }

    @Test
    void windowSlidesOnServerClock_allowsAgainAfterExpiry() throws InterruptedException {
        String rateKey = "{tenant:" + UUID.randomUUID() + "}:rate";
        List<String> keys = List.of(rateKey, rateKey + ":seq");
        long window = 800L;   // short but real, so the test stays fast
        int limit = 1;

        assertThat(run(keys, window, limit)).isEqualTo(0L);    // consumed the only slot
        assertThat(run(keys, window, limit)).isEqualTo(-1L);   // breached within the window
        Thread.sleep(window + 300L);                           // let the server-clock window elapse
        assertThat(run(keys, window, limit))
                .as("after the window elapses on the server clock, the slot frees up")
                .isEqualTo(0L);
    }

    private static Long run(List<String> keys, long windowMs, int limit) {
        // Tests may block — the .block() ban is on edge-gateway/src/main only (GatewayRulesTest).
        return redis.execute(SCRIPT, keys, Long.toString(windowMs), Integer.toString(limit))
                .blockLast(Duration.ofSeconds(5));
    }

    private static RedisScript<Long> script() {
        DefaultRedisScript<Long> s = new DefaultRedisScript<>();
        s.setScriptSource(new ResourceScriptSource(new ClassPathResource("scripts/sliding_window.lua")));
        s.setResultType(Long.class);
        return s;
    }
}
