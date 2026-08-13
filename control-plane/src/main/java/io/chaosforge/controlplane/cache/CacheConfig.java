package io.chaosforge.controlplane.cache;

import io.chaosforge.controlplane.domain.Scenario;
import io.chaosforge.controlplane.domain.Tenant;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Map;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import tools.jackson.databind.ObjectMapper;

/**
 * Two-level caches + the Redis invalidation bus. TTLs encode the documented worst-case staleness when
 * the bus is down: tenants L2 = 1h, scenarios L2 = 5m. rule_sets are deliberately absent — they are
 * append-only and never invalidated (ADR-0503); their cache lives in the Execution Service.
 */
@Configuration
public class CacheConfig {

    @Bean
    public TwoLevelCache<Tenant> tenantCache(StringRedisTemplate redis, ObjectMapper mapper, MeterRegistry registry) {
        return new TwoLevelCache<>("tenants", Tenant.class, Duration.ofMinutes(5), Duration.ofHours(1),
                redis, mapper, registry);
    }

    @Bean
    public TwoLevelCache<Scenario> scenarioCache(StringRedisTemplate redis, ObjectMapper mapper, MeterRegistry registry) {
        return new TwoLevelCache<>("scenarios", Scenario.class, Duration.ofMinutes(1), Duration.ofMinutes(5),
                redis, mapper, registry);
    }

    @Bean
    public CacheInvalidationListener cacheInvalidationListener(
            TwoLevelCache<Tenant> tenantCache, TwoLevelCache<Scenario> scenarioCache) {
        return new CacheInvalidationListener(Map.of(
                tenantCache.name(), tenantCache,
                scenarioCache.name(), scenarioCache));
    }

    @Bean
    public RedisMessageListenerContainer cacheInvalidationContainer(
            RedisConnectionFactory connectionFactory, CacheInvalidationListener listener) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(listener, new ChannelTopic(CacheInvalidationPublisher.CHANNEL));
        return container;
    }
}
