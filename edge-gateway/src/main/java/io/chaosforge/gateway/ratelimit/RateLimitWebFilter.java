package io.chaosforge.gateway.ratelimit;

import io.chaosforge.gateway.cache.TenantPolicy;
import io.chaosforge.gateway.cache.TenantPolicyCache;
import io.chaosforge.gateway.security.TenantContext;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.micrometer.core.instrument.MeterRegistry;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import org.springframework.web.util.pattern.PathPattern;
import org.springframework.web.util.pattern.PathPatternParser;
import reactor.core.publisher.Mono;

/**
 * Two rate-limit layers on one atomic Redis Lua sliding window (Lettuce reactive, no blocking calls):
 * a per-tenant limit on every request (true-global single shared key, not per-pod; hash-tagged
 * {@code {tenant:<id>}:rate} for Redis Cluster co-location), and an extra per-bearer-token limit on
 * operate-scoped {@code /mcp} requests (ADR-0543). The token limit runs first; its rejection does not
 * consume the tenant limit.
 *
 * <p><b>Tenant limiter fails open by design</b> (ADR-0540): if Redis or the policy lookup errors, the
 * request is allowed through — availability is favoured over rate-limit correctness during a Redis
 * outage. That choice is <b>instrumented and logged</b> (arch-audit H3): every decision increments
 * {@code chaosforge.gateway.rate_limit{outcome,limiter}} (allowed / rate_limited / fail_open /
 * local_fallback), and a fail-open event WARNs.
 *
 * <p><b>Operate-token limiter degrades to a local limit</b> (ADR-0543 Amendment 2): Redis calls carry a
 * timeout and circuit breaker; on failure the token limit is enforced on a per-pod window instead of
 * being skipped. Per-pod means an upper bound (N pods = N × limit), not an exact limit.
 */
@Component
public class RateLimitWebFilter implements WebFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(RateLimitWebFilter.class);
    private static final long WINDOW_MS = 60_000L;
    private static final String METRIC = "chaosforge.gateway.rate_limit";
    private static final String ALLOWED = "allowed";
    private static final String RATE_LIMITED = "rate_limited";
    private static final String FAIL_OPEN = "fail_open";
    private static final String LOCAL_FALLBACK = "local_fallback";
    private static final String TENANT = "tenant";
    private static final String TOKEN = "token";

    private final int operateTokenRateLimit;
    private static final PathPattern MCP_PATH = PathPatternParser.defaultInstance.parse("/mcp");

    private final ReactiveStringRedisTemplate redis;
    private final RedisScript<Long> rateLimitScript;
    private final TenantPolicyCache policyCache;
    private final MeterRegistry meterRegistry;
    private final CircuitBreaker redisCb;
    private final Duration redisTimeout;
    // Fixed window from first hit; eviction resets that token's count.
    private final Cache<String, AtomicInteger> localTokenWindow = Caffeine.newBuilder()
            .maximumSize(10_000)
            .expireAfterWrite(Duration.ofMillis(WINDOW_MS))
            .build();

    public RateLimitWebFilter(
            ReactiveStringRedisTemplate redis,
            RedisScript<Long> rateLimitScript,
            TenantPolicyCache policyCache,
            MeterRegistry meterRegistry,
            CircuitBreakerRegistry cbRegistry,
            @Value("${chaosforge.gateway.rate-limit.redis-timeout-ms:250}")
            long redisTimeoutMs,
            @Value("${chaosforge.security.mcp.operate-rate-limit-per-token}")
            int operateTokenRateLimit) {
        this.redis = redis;
        this.rateLimitScript = rateLimitScript;
        this.policyCache = policyCache;
        this.meterRegistry = meterRegistry;
        this.redisCb = cbRegistry.circuitBreaker("gateway-ratelimit-redis");
        this.redisTimeout = Duration.ofMillis(redisTimeoutMs);
        this.operateTokenRateLimit = operateTokenRateLimit;
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
            // Operate-scoped /mcp: token limit first; a rejection skips the tenant limiter.
            return ReactiveSecurityContextHolder.getContext()
                    .map(context -> context.getAuthentication())
                    .filter(authentication -> isMcpOperateRequest(exchange, authentication))
                    .flatMap(authentication -> enforceOperateTokenLimit(
                            authentication, exchange, tenantId))
                    .defaultIfEmpty(true)
                    .flatMap(tokenAllowed -> {
                        if (!tokenAllowed) {
                            return Mono.empty();
                        }

                        return policyCache.get(tenantId)
                                .flatMap(policy -> enforce(tenantId, policy, exchange, chain))
                                .onErrorResume(e -> failOpenAllow(tenantId, e, exchange, chain));
                    });
        });
    }

    private Mono<Void> enforce(UUID tenantId, TenantPolicy policy, ServerWebExchange exchange, WebFilterChain chain) {
        // Hash-tagged keys share one Redis Cluster slot; :seq is a declared key, not script-computed.
        String rateKey = "{tenant:" + tenantId + "}:rate";
        List<String> keys = List.of(rateKey, rateKey + ":seq");
        // Window clock is sourced inside the script from redis TIME (GAP-03) to avoid pod clock drift.
        return evalWindow(keys, policy.rateLimitPerMin())
                .flatMap(remaining -> {
                    if (remaining < 0) {
                        count(RATE_LIMITED, TENANT);
                        return reject(exchange);
                    }
                    count(ALLOWED, TENANT);
                    return chain.filter(exchange);
                });
    }

    /** Shared Redis call for both limiters: bounded by timeout and circuit breaker. */
    private Mono<Long> evalWindow(List<String> keys, int limit) {
        return redis.execute(rateLimitScript, keys,
                        Long.toString(WINDOW_MS),
                        Integer.toString(limit))
                .next()
                .defaultIfEmpty(0L)
                .timeout(redisTimeout)   // hung Redis must error, or fallback never fires
                .transformDeferred(CircuitBreakerOperator.of(redisCb));
    }

    private void count(String outcome, String limiter) {
        // Single emit point keeps tag keys consistent across all outcomes.
        meterRegistry.counter(METRIC, "outcome", outcome, "limiter", limiter).increment();
    }

    /** Redis / policy lookup failed — allow the request (fail-open) but make it visible, never silent. */
    private Mono<Void> failOpenAllow(UUID tenantId, Throwable e, ServerWebExchange exchange, WebFilterChain chain) {
        count(FAIL_OPEN, TENANT);
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

    private static boolean isMcpOperateRequest(
            ServerWebExchange exchange,
            Authentication authentication) {

        // PathPattern ignores matrix params; exact /mcp only, /mcp/ not matched (ADR-0543).
        if (!MCP_PATH.matches(exchange.getRequest().getPath().pathWithinApplication())) {
            return false;
        }

        return authentication.getAuthorities().stream()
                .anyMatch(authority ->
                        "SCOPE_chaosforge.operate".equals(authority.getAuthority()));
    }

    private Mono<Boolean> enforceOperateTokenLimit(
            Authentication authentication,
            ServerWebExchange exchange,
            UUID tenantId) {

        if (!(authentication instanceof JwtAuthenticationToken jwtAuthentication)) {
            return Mono.just(true);
        }

        // Per-bearer bucket: do not collapse distinct credentials into one tenant:sub bucket.
        String fingerprint = sha256(jwtAuthentication.getToken().getTokenValue());
        String rateKey = "{mcp-token:" + fingerprint + "}:rate";
        List<String> keys = List.of(rateKey, rateKey + ":seq");

        return evalWindow(keys, operateTokenRateLimit)
                .flatMap(remaining -> {
                    if (remaining < 0) {
                        count(RATE_LIMITED, TOKEN);
                        return reject(exchange).thenReturn(false);
                    }
                    return Mono.just(true);
                })
                .onErrorResume(e -> localTokenDecision(fingerprint, exchange, tenantId, e));
    }

    /** Redis unavailable: enforce the same limit per pod instead of skipping it. */
    private Mono<Boolean> localTokenDecision(
            String fingerprint,
            ServerWebExchange exchange,
            UUID tenantId,
            Throwable e) {

        // Not fail_open: the request is still throttled here.
        count(LOCAL_FALLBACK, TOKEN);
        log.warn("rate-limit Redis unavailable ({}) — operate token on local per-pod limit, tenant …{}",
                e.getClass().getSimpleName(), last4(tenantId));

        int used = localTokenWindow.get(fingerprint, k -> new AtomicInteger()).incrementAndGet();
        if (used > operateTokenRateLimit) {
            count(RATE_LIMITED, TOKEN);
            return reject(exchange).thenReturn(false);   // 429 + Retry-After, same as Redis path
        }
        return Mono.just(true);
    }

    private static String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(
                    digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
