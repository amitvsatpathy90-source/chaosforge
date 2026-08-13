package io.chaosforge.execution.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.chaosforge.execution.outbox.ExecOutboxMetrics;
import io.chaosforge.execution.outbox.ExecOutboxPoller;
import io.chaosforge.execution.outbox.ExecOutboxRelayDao;
import io.chaosforge.execution.pipeline.ScenarioCommandListener;
import io.chaosforge.execution.sweep.IncompleteRunSweeper;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Full-context wiring smoke test (production-grade belt for the Defect A/B beans). Boots the entire
 * Spring application against a real Postgres (Testcontainers + the V4–V7 Flyway migrations) and an
 * embedded Kafka broker, then asserts the new outbox-relay and INCOMPLETE-sweep beans are wired and
 * the relay SLI gauges are registered. This is the coverage the component ITs deliberately skip:
 * it proves constructor injection (KafkaTemplate, PlatformTransactionManager), @Scheduled enablement,
 * the @KafkaListener container, and the security filter chain all come up together.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 1, topics = {
        "chaosforge.scenario.commands.v1",
        "chaosforge.scenario.results.v1"})
@Testcontainers
class ExecutionServiceContextIT {

    @Container
    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16.4-alpine");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", PG::getJdbcUrl);
        registry.add("spring.datasource.username", PG::getUsername);
        registry.add("spring.datasource.password", PG::getPassword);
        // Lazy JWK decoder — built but never fetched during a context-load test, so a dummy URI is safe.
        registry.add("spring.security.oauth2.resourceserver.jwt.jwk-set-uri",
                () -> "http://localhost:0/.well-known/jwks.json");
    }

    @Autowired
    private MeterRegistry meterRegistry;

    @Test
    void contextLoads_relayAndSweepBeansWired(@Autowired ExecOutboxPoller poller,
                                              @Autowired ExecOutboxRelayDao relayDao,
                                              @Autowired ExecOutboxMetrics metrics,
                                              @Autowired IncompleteRunSweeper sweeper,
                                              @Autowired ScenarioCommandListener listener) {
        assertThat(poller).isNotNull();
        assertThat(relayDao).isNotNull();
        assertThat(metrics).isNotNull();
        assertThat(sweeper).isNotNull();
        assertThat(listener).isNotNull();

        // ExecOutboxMetrics actually registered its gauges against the real registry.
        assertThat(meterRegistry.find("outbox.pending_count").gauge()).isNotNull();
        assertThat(meterRegistry.find("outbox.dead_count").gauge()).isNotNull();
        assertThat(meterRegistry.find("outbox.oldest_pending_age_seconds").gauge()).isNotNull();
    }
}
