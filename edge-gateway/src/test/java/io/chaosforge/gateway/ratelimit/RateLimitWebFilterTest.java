package io.chaosforge.gateway.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.chaosforge.gateway.cache.TenantPolicy;
import io.chaosforge.gateway.cache.TenantPolicyCache;
import io.chaosforge.gateway.security.TenantContext;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Docker-free coverage for the rate-limit filter's control flow (arch-audit H3/M3 follow-up — this
 * filter previously had zero tests). The Lua sliding-window arithmetic itself is not re-verified here
 * (that needs a real Redis); what's under test is the Java decision logic the fail-open instrumentation
 * depends on: allow / reject / fail-open are mutually exclusive, each increments exactly the counter the
 * {@code TenantPolicyFeedDown}/{@code RateLimitFailingOpen} alerts (ADR-0537) read, and a fail-open event
 * actually WARNs (the specific claim this filter's Javadoc makes).
 */
class RateLimitWebFilterTest {

    private static final UUID TENANT_ID = UUID.randomUUID();
    private static final TenantPolicy POLICY = new TenantPolicy(TENANT_ID, 100);

    @SuppressWarnings("unchecked")
    private final RedisScript<Long> script = mock(RedisScript.class);
    private final ReactiveStringRedisTemplate redis = mock(ReactiveStringRedisTemplate.class);
    private final TenantPolicyCache policyCache = mock(TenantPolicyCache.class);
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final RateLimitWebFilter filter = new RateLimitWebFilter(redis, script, policyCache, registry);

    private final WebFilterChain chain = mock(WebFilterChain.class);
    private ListAppender<ILoggingEvent> logAppender;

    @BeforeEach
    void setUp() {
        when(chain.filter(any())).thenReturn(Mono.empty());
        when(policyCache.get(TENANT_ID)).thenReturn(Mono.just(POLICY));

        logAppender = new ListAppender<>();
        logAppender.start();
        ((Logger) LoggerFactory.getLogger(RateLimitWebFilter.class)).addAppender(logAppender);
    }

    @AfterEach
    void tearDown() {
        ((Logger) LoggerFactory.getLogger(RateLimitWebFilter.class)).detachAppender(logAppender);
    }

    @Test
    @SuppressWarnings("unchecked")
    void underLimit_forwardsToChain_andCountsAllowed() {
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class)))
                .thenReturn(Flux.just(42L));   // non-negative remaining → allowed

        StepVerifier.create(withTenant(filter.filter(exchange(), chain))).verifyComplete();

        verify(chain, times(1)).filter(any());
        assertThat(counter("allowed")).isEqualTo(1.0);
        assertThat(counter("rate_limited")).isZero();
        assertThat(counter("fail_open")).isZero();
    }

    @Test
    @SuppressWarnings("unchecked")
    void overLimit_rejectsWith429AndRetryAfter_neverForwardsToChain_andCountsRateLimited() {
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class)))
                .thenReturn(Flux.just(-1L));   // breach sentinel

        ServerWebExchange exchange = exchange();
        StepVerifier.create(withTenant(filter.filter(exchange, chain))).verifyComplete();

        verify(chain, never()).filter(any());
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(exchange.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isEqualTo("60");
        assertThat(counter("rate_limited")).isEqualTo(1.0);
        assertThat(counter("allowed")).isZero();
        assertThat(counter("fail_open")).isZero();
    }

    @Test
    @SuppressWarnings("unchecked")
    void redisError_failsOpen_forwardsToChain_countsFailOpen_andWarns() {
        // The exact regression this filter's instrumentation exists to catch: pre-fix this path was
        // silent — request allowed, nothing observable. Now it must be counted AND logged.
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class)))
                .thenReturn(Flux.error(new RuntimeException("connection refused")));

        StepVerifier.create(withTenant(filter.filter(exchange(), chain))).verifyComplete();

        verify(chain, times(1)).filter(any());
        assertThat(counter("fail_open")).isEqualTo(1.0);
        assertThat(counter("allowed")).isZero();
        assertThat(counter("rate_limited")).isZero();
        assertThat(logAppender.list)
                .as("a fail-open event must WARN, not fail silently")
                .anySatisfy(event -> {
                    assertThat(event.getLevel()).isEqualTo(Level.WARN);
                    assertThat(event.getFormattedMessage()).contains("fail-open");
                });
    }

    @Test
    void policyLookupError_alsoFailsOpen_countsFailOpen() {
        // onErrorResume wraps the whole policyCache.get(...).flatMap(...) chain — a broken policy feed
        // (arch-audit H1/TenantPolicyFeedDown) must fail open exactly like a broken Redis, not just the
        // enforce() half.
        when(policyCache.get(TENANT_ID)).thenReturn(Mono.error(new RuntimeException("CP unreachable")));

        StepVerifier.create(withTenant(filter.filter(exchange(), chain))).verifyComplete();

        verify(chain, times(1)).filter(any());
        assertThat(counter("fail_open")).isEqualTo(1.0);
    }

    @Test
    void noTenantInContext_bypassesRateLimiting_entirely() {
        // The actuator/unauthenticated path: no TenantContext means no rate-limit decision at all —
        // not "allowed", not counted, Redis never touched.
        StepVerifier.create(filter.filter(exchange(), chain)).verifyComplete();   // no withTenant()

        verify(chain, times(1)).filter(any());
        assertThat(counter("allowed")).isZero();
        assertThat(counter("rate_limited")).isZero();
        assertThat(counter("fail_open")).isZero();
    }

    private static Mono<Void> withTenant(Mono<Void> mono) {
        return mono.contextWrite(ctx -> ctx.put(TenantContext.KEY, TENANT_ID));
    }

    private static ServerWebExchange exchange() {
        return MockServerWebExchange.from(MockServerHttpRequest.get("/v1/scenarios").build());
    }

    private double counter(String outcome) {
        return registry.counter("chaosforge.gateway.rate_limit", "outcome", outcome).count();
    }
}
