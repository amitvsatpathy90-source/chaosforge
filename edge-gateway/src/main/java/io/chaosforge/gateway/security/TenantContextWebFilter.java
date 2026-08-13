package io.chaosforge.gateway.security;

import java.util.Optional;
import java.util.UUID;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/**
 * Extracts {@code tenant_id} from the verified JWT and writes it into the Reactor Context (ADR-0509
 * Layer 1). Runs after Spring Security authentication. Unauthenticated paths (actuator) pass through; an
 * authenticated token with no parseable {@code tenant_id} is rejected 403 (SEC-03) so it can never reach
 * the rate limiter's no-tenant bypass and hit the Control Plane unthrottled.
 */
@Component
public class TenantContextWebFilter implements WebFilter, Ordered {

    @Override
    public int getOrder() {
        return 1;   // after the security filter chain (WebFilterChainProxy is at -100)
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        // No switchIfEmpty after the terminal Mono<Void> — it previously double-forwarded requests.
        // defaultIfEmpty covers pass-through/forward/reject in one flatMap, no trailing emptiness check.
        return ReactiveSecurityContextHolder.getContext()
                .map(ctx -> Optional.ofNullable(ctx.getAuthentication()))
                .defaultIfEmpty(Optional.empty())
                .flatMap(maybeAuth -> {
                    if (maybeAuth.orElse(null) instanceof JwtAuthenticationToken jwtAuth) {
                        return forwardOrReject(jwtAuth.getToken(), exchange, chain);
                    }
                    return chain.filter(exchange);   // no JWT auth (e.g. a permitted actuator path)
                });
    }

    private Mono<Void> forwardOrReject(Jwt jwt, ServerWebExchange exchange, WebFilterChain chain) {
        UUID tenantId;
        try {
            tenantId = UUID.fromString(jwt.getClaimAsString("tenant_id"));   // null claim → NPE; malformed → IAE
        } catch (RuntimeException e) {
            // SEC-03: no parseable tenant_id — reject; else falls into the rate limiter's no-tenant bypass.
            exchange.getResponse().setStatusCode(HttpStatus.FORBIDDEN);
            return exchange.getResponse().setComplete();
        }
        return chain.filter(exchange).contextWrite(ctx -> ctx.put(TenantContext.KEY, tenantId));
    }
}
