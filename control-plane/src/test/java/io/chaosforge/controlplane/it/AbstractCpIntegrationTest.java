package io.chaosforge.controlplane.it;

import org.mockito.Mockito;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Base for full-context Control Plane integration tests. Boots the entire Spring application against
 * real backing services so wiring, lifecycle beans, and the Flyway schema are all exercised together:
 *
 * <ul>
 *   <li><b>Postgres</b> — DataSource + JdbcTemplate + Flyway (V1–V8) + the @Scheduled outbox poller.</li>
 *   <li><b>Redis</b> — satisfies the {@code RedisMessageListenerContainer} (a {@code SmartLifecycle}
 *       that opens a subscription at context refresh) and the two-level cache.</li>
 *   <li><b>Embedded Kafka</b> — the outbox producer's broker.</li>
 *   <li><b>Spring AI Ollama</b> — neutralized: a {@code @Primary} mock {@link ChatClient.Builder}
 *       takes precedence over the autoconfigured one, so no bean ever reaches a real Ollama endpoint
 *       at boot (the autoconfigured Ollama beans construct lazily and never connect).</li>
 * </ul>
 *
 * <p><b>Container lifecycle:</b> the Postgres and Redis containers are JVM-wide singletons started
 * once in a static initializer and reaped by the Testcontainers Ryuk sidecar at JVM exit. They are
 * deliberately <em>not</em> {@code @Container}-managed (which would start/stop per class): every
 * concrete subclass shares one Spring context (cached by Spring's {@code TestContext} framework) and
 * one set of containers — no container leakage across the suite, no per-class startup cost.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 1, topics = "chaosforge.scenario.commands.v1")
@Import(AbstractCpIntegrationTest.CpTestSupportConfig.class)
abstract class AbstractCpIntegrationTest {

    @SuppressWarnings("resource")   // Ryuk reaps these JVM-wide singletons at exit — no explicit close.
    private static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16.4-alpine"));

    @SuppressWarnings("resource")
    private static final GenericContainer<?> REDIS =
            // Pin the exact tag compose runs (redis:7.4-alpine), not a floating minor — a moving tag
            // makes this IT's environment non-reproducible, the same hazard the no-SNAPSHOT rule bars.
            new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine")).withExposedPorts(6379);

    static {
        // Start in parallel-friendly order; both are required before the Spring context refreshes.
        POSTGRES.start();
        REDIS.start();
    }

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        // Lazy JWK decoder — built but never fetched during these tests, so a dummy URI is safe.
        registry.add("spring.security.oauth2.resourceserver.jwt.jwk-set-uri",
                () -> "http://localhost:0/.well-known/jwks.json");
        // Defensive: keep any Ollama bean construction off a real endpoint (it never connects at boot).
        registry.add("spring.ai.ollama.base-url", () -> "http://localhost:0");
    }

    /**
     * Neutralizes Spring AI at the wiring boundary. A {@code @Primary} mock {@link ChatClient.Builder}
     * is preferred when {@code AiConfig} injects a builder, and its {@code build()} yields a mock
     * {@link ChatClient} — so the authoring/triage beans wire without an Ollama server present.
     */
    @TestConfiguration(proxyBeanMethods = false)
    static class CpTestSupportConfig {

        @Bean
        @Primary
        ChatClient.Builder testChatClientBuilder() {
            ChatClient.Builder builder = Mockito.mock(ChatClient.Builder.class);
            Mockito.when(builder.build()).thenReturn(Mockito.mock(ChatClient.class));
            return builder;
        }
    }
}
