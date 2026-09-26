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
 * An unauthenticated /mcp call must 401 with a WWW-Authenticate pointing at this Gateway's own
 * PRM endpoint (RFC 9728 discovery), and that endpoint must actually be reachable and correct.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "management.server.port=0")
@Import(McpUnauthorizedResponseIT.WebClientBuilderConfig.class)
class McpUnauthorizedResponseIT {

    @Autowired
    private Environment environment;

    @TestConfiguration(proxyBeanMethods = false)
    static class WebClientBuilderConfig {
        @Bean
        WebClient.Builder webClientBuilder() {
            return WebClient.builder();
        }
    }

    @DynamicPropertySource
    static void lazyEndpoints(DynamicPropertyRegistry registry) {
        registry.add("spring.security.oauth2.resourceserver.jwt.jwk-set-uri",
                () -> "http://localhost:0/.well-known/jwks.json");
        registry.add("chaosforge.control-plane.base-url", () -> "http://localhost:0");
    }

    @Test
    void unauthenticatedMcpCall_401sWithResourceMetadataPointingAtThisGateway() {
        int port = environment.getProperty("local.server.port", Integer.class);
        String base = "http://localhost:" + port;

        RestClient.create().post().uri(base + "/mcp")
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode().value()).isEqualTo(401);
                    String wwwAuthenticate = response.getHeaders().getFirst("WWW-Authenticate");
                    assertThat(wwwAuthenticate)
                            .contains("resource_metadata=\"" + base + "/.well-known/oauth-protected-resource\"");
                    return null;
                });

        RestClient.create().get().uri(base + "/.well-known/oauth-protected-resource")
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode().value()).isEqualTo(200);
                    String body = new String(response.getBody().readAllBytes());
                    assertThat(body).contains("\"resource\":\"" + base + "/mcp\"");
                    return null;
                });
    }
}

