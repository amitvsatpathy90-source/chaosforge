package io.chaosforge.controlplane.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.chaosforge.controlplane.ai.ScenarioAuthoringService;
import io.chaosforge.controlplane.cache.CacheInvalidationListener;
import io.chaosforge.controlplane.janitor.ReplayIdempotencyJanitor;
import io.chaosforge.controlplane.outbox.OutboxMetrics;
import io.chaosforge.controlplane.outbox.OutboxPoller;
import io.chaosforge.controlplane.outbox.OutboxRelayDao;
import io.chaosforge.controlplane.replay.ScenarioReplayOrchestrator;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/**
 * Full-context wiring smoke test (the coverage the no-context DAO ITs deliberately skip): the whole
 * Control Plane comes up against real Postgres + Redis + embedded Kafka, with Spring AI mocked. Proves
 * constructor injection (KafkaTemplate, transaction manager, Redis), @Scheduled enablement, the Redis
 * invalidation subscription lifecycle, the security filter chain, and the outbox SLI gauges all start
 * together — so a future bean-wiring regression fails here instead of at deploy time.
 */
class ControlPlaneContextIT extends AbstractCpIntegrationTest {

    @Test
    void contextLoads_coreBeansWired(@Autowired OutboxPoller poller,
                                     @Autowired OutboxRelayDao relayDao,
                                     @Autowired OutboxMetrics outboxMetrics,
                                     @Autowired ScenarioReplayOrchestrator orchestrator,
                                     @Autowired CacheInvalidationListener cacheListener,
                                     @Autowired ScenarioAuthoringService authoringService,
                                     @Autowired ReplayIdempotencyJanitor janitor,
                                     @Autowired @Qualifier("cacheInvalidationContainer")
                                             RedisMessageListenerContainer redisListener,
                                     @Autowired MeterRegistry meterRegistry) {
        assertThat(poller).isNotNull();
        assertThat(relayDao).isNotNull();
        assertThat(outboxMetrics).isNotNull();
        assertThat(orchestrator).isNotNull();
        assertThat(cacheListener).isNotNull();
        assertThat(authoringService).isNotNull();
        assertThat(janitor).as("ADR-0528 replay_idempotency janitor is wired and scheduled").isNotNull();

        // The Redis pub/sub invalidation subscription actually started (proves Redis connectivity).
        assertThat(redisListener.isRunning()).as("Redis invalidation subscription is live").isTrue();

        // OutboxMetrics registered its SLI gauges against the real registry.
        assertThat(meterRegistry.find("outbox.pending_count").gauge()).isNotNull();
        assertThat(meterRegistry.find("outbox.dead_count").gauge()).isNotNull();
        assertThat(meterRegistry.find("outbox.oldest_pending_age_seconds").gauge()).isNotNull();
    }
}
