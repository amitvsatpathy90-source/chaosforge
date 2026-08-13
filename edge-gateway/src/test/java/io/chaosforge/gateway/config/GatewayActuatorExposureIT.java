package io.chaosforge.gateway.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestClient;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * SEC-04 (grounded): boots the full gateway and proves {@code /actuator/prometheus} is served on the
 * PRIVATE management port, never the public ingress port. Before this the metrics endpoint sat on 8080
 * (the sole internet-facing listener), leaking CB states / the rate-limit {@code fail_open} counter /
 * JVM internals unauthenticated. This also confirms the whole context boots with the Batch-2 changes,
 * and empirically confirms the WebFlux management-port split behaves as intended.
 *
 * <p>The management port is randomized ({@code management.server.port=0}) so the test never collides
 * with the fixed 9080 in {@code application.yml}. No live Redis/JWKS is needed — the connection factory
 * and JWK decoder both initialize lazily, and {@code /actuator/prometheus} runs no health indicators.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "management.server.port=0")
@Import(GatewayActuatorExposureIT.WebClientBuilderConfig.class)
class GatewayActuatorExposureIT {

    @Autowired
    private Environment environment;

    /**
     * The full context wires {@code controlPlaneWebClient} from an autoconfigured {@code WebClient.Builder}
     * that this test slice doesn't supply (the running app gets it at runtime). Provide one so the context
     * boots — this test asserts the actuator PORT split, which never touches the WebClient. Backed off by
     * {@code @ConditionalOnMissingBean} if autoconfig does provide it.
     */
    @TestConfiguration(proxyBeanMethods = false)
    static class WebClientBuilderConfig {
        @Bean
        WebClient.Builder webClientBuilder() {
            return WebClient.builder();
        }
    }

    @DynamicPropertySource
    static void lazyEndpoints(DynamicPropertyRegistry registry) {
        // Nothing fetches these during the test; point them at dead ports so no external call is attempted.
        registry.add("spring.security.oauth2.resourceserver.jwt.jwk-set-uri",
                () -> "http://localhost:0/.well-known/jwks.json");
        registry.add("chaosforge.control-plane.base-url", () -> "http://localhost:0");
    }

    @Test
    void prometheus_isOnTheManagementPort_notThePublicIngress() {
        int publicPort = port("local.server.port");
        int managementPort = port("local.management.port");
        assertThat(managementPort).as("a separate management port must be active").isNotEqualTo(publicPort);

        assertThat(status("http://localhost:" + publicPort + "/actuator/prometheus"))
                .as("SEC-04: metrics must NOT be reachable on the public ingress port")
                .isNotEqualTo(200);
        assertThat(status("http://localhost:" + managementPort + "/actuator/prometheus"))
                .as("metrics ARE served on the private management port (for the Prometheus scrape)")
                .isEqualTo(200);
    }

    private int port(String key) {
        Integer p = environment.getProperty(key, Integer.class);
        assertThat(p).as(key + " must be published").isNotNull();
        return p;
    }

    /** Raw status, no throw on 4xx/5xx. */
    private static int status(String url) {
        return RestClient.create().get().uri(url).exchange((request, response) -> response.getStatusCode().value());
    }
}
