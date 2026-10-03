package io.chaosforge.gateway.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.web.client.RestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "management.server.port=0")
class McpOperateRateLimitIT {

    @Autowired private Environment environment;
    @MockitoBean(name = "mcpJwtDecoder") private ReactiveJwtDecoder mcpJwtDecoder;
    @MockitoBean private ReactiveStringRedisTemplate redis;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.security.oauth2.resourceserver.jwt.jwk-set-uri",
                () -> "http://localhost:0/.well-known/jwks.json");
        registry.add("chaosforge.control-plane.base-url", () -> "http://localhost:0");
    }

    @BeforeEach
    @SuppressWarnings("unchecked")
    void stub() {
        Jwt jwt = Jwt.withTokenValue("operate-token")
                .header("alg", "RS256")
                .claim("sub", "agent-1")
                .claim("tenant_id", UUID.randomUUID().toString())
                .claim("scope", "chaosforge.operate")
                .build();
        when(mcpJwtDecoder.decode(anyString())).thenReturn(Mono.just(jwt));
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class)))
                .thenReturn(Flux.just(-1L));   // every bucket breached
    }

    private int post(String path) {
        String base = "http://localhost:" + environment.getProperty("local.server.port");
        return RestClient.create().post().uri(base + path)
                .header("Authorization", "Bearer operate-token")
                .header("Content-Type", "application/json")
                .body("{}")
                .exchange((req, res) -> res.getStatusCode().value());
    }

    @Test
    void operateScopeClaim_mapsToAuthority_andTokenLimiterReturns429() {
        assertThat(post("/mcp")).isEqualTo(429);
    }
}
