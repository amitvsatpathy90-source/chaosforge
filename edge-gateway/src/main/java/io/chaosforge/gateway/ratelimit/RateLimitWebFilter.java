package io.chaosforge.gateway.ratelimit;

import io.chaosforge.gateway.cache.TenantPolicy;
import io.chaosforge.gateway.cache.TenantPolicyCache;
import io.chaosforge.gateway.security.TenantContext;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/**
 * True-global per-tenant rate limiting via an atomic Redis Lua sliding window (single shared key,
 * not per-pod). Hash-tagged {@code {tenant:<id>}:rate} for Redis Cluster co-location. Lettuce
 * reactive only — no blocking calls.
 *
 * <p><b>Fail-open by design</b> (architecture specifications): if Redis or the policy lookup errors, the request is
 * allowed through — availability is favoured over rate-limit correctness during a Redis outage. That
 * choice is now <b>instrumented and logged</b> (arch-audit H3): every decision increments
 * {@code chaosforge.gateway.rate_limit{outcome}} (allowed / rate_limited / fail_open), and a fail-open
 * event WARNs. A silent fail-open means rate limiting can be globally disabled by a Redis blip with no
 * signal; the {@code fail_open} counter makes that an alertable condition.
 */
@Component
public class RateLimitWebFilter implements WebFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(RateLimitWebFilter.class);
    private static final long WINDOW_MS = 60_000L;
    private static final String METRIC = "chaosforge.gateway.rate_limit";

    private final ReactiveStringRedisTemplate redis;
    private final RedisScript<Long> rateLimitScript;
    private final TenantPolicyCache policyCache;
    private final Counter allowed;
    private final Counter rateLimited;
    private final Counter failOpen;

    public RateLimitWebFilter(ReactiveStringRedisTemplate redis, RedisScript<Long> rateLimitScript,
                              TenantPolicyCache policyCache, MeterRegistry meterRegistry) {
        this.redis = redis;
        this.rateLimitScript = rateLimitScript;
        this.policyCache = policyCache;
        this.allowed = meterRegistry.counter(METRIC, "outcome", "allowed");
        this.rateLimited = meterRegistry.counter(METRIC, "outcome", "rate_limited");
        this.failOpen = meterRegistry.counter(METRIC, "outcome", "fail_open");
    }

    @Override
    public int getOrder() {
        return 2;   // after TenantContextWebFilter (order 1) so the tenant id is in context
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        return Mono.deferContextual(ctx -> {
            if (!ctx.hasKey(TenantContext.KEY)) {
                return chain.filter(exchange);   // unauthenticated path (actuator) — not rate limited
            }
            UUID tenantId = ctx.get(TenantContext.KEY);
            return policyCache.get(tenantId)
                    .flatMap(policy -> enforce(tenantId, policy, exchange, chain))
                    .onErrorResume(e -> failOpenAllow(tenantId, e, exchange, chain));
        });
    }

    private Mono<Void> enforce(UUID tenantId, TenantPolicy policy, ServerWebExchange exchange, WebFilterChain chain) {
        // Hash-tagged keys share one Redis Cluster slot; :seq is a declared key, not script-computed.
        String rateKey = "{tenant:" + tenantId + "}:rate";
        List<String> keys = List.of(rateKey, rateKey + ":seq");
        // Window clock is sourced inside the script from redis TIME (GAP-03) to avoid pod clock drift.
        return redis.execute(rateLimitScript, keys,
                        Long.toString(WINDOW_MS),
                        Integer.toString(policy.rateLimitPerMin()))
                .next()
                .defaultIfEmpty(0L)
                .flatMap(remaining -> {
                    if (remaining < 0) {
                        rateLimited.increment();
                        return reject(exchange);
                    }
                    allowed.increment();
                    return chain.filter(exchange);
                });
    }

    /** Redis / policy lookup failed — allow the request (fail-open) but make it visible, never silent. */
    private Mono<Void> failOpenAllow(UUID tenantId, Throwable e, ServerWebExchange exchange, WebFilterChain chain) {
        failOpen.increment();
        // Cause class + tenant last-4 only (PII rule — gateway-rules.md); never the full tenant id.
        log.warn("rate-limit fail-open ({}) — request allowed unthrottled for tenant …{}",
                e.getClass().getSimpleName(), last4(tenantId));
        return chain.filter(exchange);
    }

    private static Mono<Void> reject(ServerWebExchange exchange) {
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
        response.getHeaders().add(HttpHeaders.RETRY_AFTER, Long.toString(WINDOW_MS / 1000));
        return response.setComplete();
    }

    private static String last4(UUID id) {
        String s = id.toString();
        return s.substring(s.length() - 4);
    }
}
