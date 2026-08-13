package io.chaosforge.gateway.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * SEC-03: an authenticated JWT with no parseable {@code tenant_id} must be rejected (403) and NOT
 * forwarded — the downstream {@link io.chaosforge.gateway.ratelimit.RateLimitWebFilter} skips rate
 * limiting when no tenant is in context (a bypass meant only for the unauthenticated actuator path), so a
 * tenant-less token would otherwise reach the Control Plane unthrottled. The happy path must forward
 * exactly once; the no-auth path (actuator) must pass through.
 */
class TenantContextWebFilterTest {

    private final TenantContextWebFilter filter = new TenantContextWebFilter();
    private final WebFilterChain chain = mock(WebFilterChain.class);

    TenantContextWebFilterTest() {
        when(chain.filter(any())).thenReturn(Mono.empty());
    }

    @Test
    void tokenWithoutTenantClaim_is403_andNeverForwarded() {
        ServerWebExchange exchange = exchange();
        JwtAuthenticationToken auth = new JwtAuthenticationToken(jwt(null));

        StepVerifier.create(filter.filter(exchange, chain)
                        .contextWrite(ReactiveSecurityContextHolder.withAuthentication(auth)))
                .verifyComplete();

        verify(chain, never()).filter(any());
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
    }

    @Test
    void tokenWithTenantClaim_forwardsExactlyOnce() {
        ServerWebExchange exchange = exchange();
        JwtAuthenticationToken auth = new JwtAuthenticationToken(jwt(UUID.randomUUID().toString()));

        StepVerifier.create(filter.filter(exchange, chain)
                        .contextWrite(ReactiveSecurityContextHolder.withAuthentication(auth)))
                .verifyComplete();

        verify(chain, times(1)).filter(any());
        assertThat(exchange.getResponse().getStatusCode()).isNull();   // not rejected
    }

    @Test
    void noAuthentication_passesThroughOnce() {
        ServerWebExchange exchange = exchange();

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();   // empty security context

        verify(chain, times(1)).filter(any());
    }

    private static ServerWebExchange exchange() {
        return MockServerWebExchange.from(MockServerHttpRequest.get("/v1/scenarios").build());
    }

    private static Jwt jwt(String tenantId) {
        Instant now = Instant.now();
        Jwt.Builder builder = Jwt.withTokenValue("t").header("alg", "RS256").subject("sub")
                .claim("roles", List.of("USER")).issuedAt(now).expiresAt(now.plusSeconds(300));
        if (tenantId != null) {
            builder.claim("tenant_id", tenantId);
        }
        return builder.build();
    }
}
