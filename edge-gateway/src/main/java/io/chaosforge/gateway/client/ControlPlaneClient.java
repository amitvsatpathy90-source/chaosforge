package io.chaosforge.gateway.client;

import io.chaosforge.gateway.cache.TenantPolicy;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

/**
 * All outbound calls to the Control Plane. No blocking calls.
 * Proxy path (initiateReplay/getScenario): bulkhead → CB → timeout order (gateway-rules.md);
 * forwards Authorization/Idempotency-Key intact for CP's JWT re-verify + idempotency.
 * Cache-load path (fetchTenantPolicy): hits CP's /internal policy endpoint outside any request
 * context (mTLS-authenticated as the gateway process, ADR-0532); separate CB, fail-open to the
 * default policy on CP failure — never silent, WARNs + increments a fallback counter.
 */
@Component
public class ControlPlaneClient {

    private static final Logger log = LoggerFactory.getLogger(ControlPlaneClient.class);
    private static final Duration TIMEOUT = Duration.ofSeconds(3);

    private final WebClient webClient;
    private final CircuitBreaker proxyCb;
    private final Bulkhead proxyBulkhead;
    private final CircuitBreaker cacheLoadCb;
    private final Counter policyLoadSuccess;
    private final Counter policyLoadFallback;

    public ControlPlaneClient(WebClient controlPlaneWebClient, CircuitBreakerRegistry cbRegistry,
                              BulkheadRegistry bulkheadRegistry, MeterRegistry meterRegistry) {
        this.webClient = controlPlaneWebClient;
        this.proxyCb = cbRegistry.circuitBreaker("gateway-cp");
        this.proxyBulkhead = bulkheadRegistry.bulkhead("gateway-cp");
        this.cacheLoadCb = cbRegistry.circuitBreaker("cache-load");
        this.policyLoadSuccess = meterRegistry.counter("chaosforge.gateway.policy_load", "outcome", "success");
        this.policyLoadFallback = meterRegistry.counter("chaosforge.gateway.policy_load", "outcome", "fallback");
    }

    public Mono<ResponseEntity<String>> initiateReplay(UUID scenarioId, String authorization,
                                                       String ifMatch, String idempotencyKey) {
        return webClient.post().uri("/v1/scenarios/{id}:run", scenarioId)
                .header(HttpHeaders.AUTHORIZATION, authorization)
                .header(HttpHeaders.IF_MATCH, ifMatch)
                .header("Idempotency-Key", idempotencyKey)
                .exchangeToMono(ControlPlaneClient::passThrough)
                .timeout(TIMEOUT)
                .transformDeferred(CircuitBreakerOperator.of(proxyCb))
                .transformDeferred(BulkheadOperator.of(proxyBulkhead));   // outermost: bulkhead → CB → timeout
    }

    public Mono<ResponseEntity<String>> getScenario(UUID scenarioId, String authorization) {
        return webClient.get().uri("/v1/scenarios/{id}", scenarioId)
                .header(HttpHeaders.AUTHORIZATION, authorization)
                .exchangeToMono(ControlPlaneClient::passThrough)
                .timeout(TIMEOUT)
                .transformDeferred(CircuitBreakerOperator.of(proxyCb))
                .transformDeferred(BulkheadOperator.of(proxyBulkhead));
    }

    /** L1 cache loader. cache-load CB fails fast; onErrorResume keeps the rate limiter fail-open. */
    public Mono<TenantPolicy> fetchTenantPolicy(UUID tenantId) {
        return webClient.get().uri("/internal/tenants/{id}/policy", tenantId)
                .retrieve()
                .bodyToMono(TenantPolicy.class)
                .timeout(TIMEOUT)
                .transformDeferred(CircuitBreakerOperator.of(cacheLoadCb))
                .doOnNext(policy -> policyLoadSuccess.increment())
                .onErrorResume(e -> {
                    policyLoadFallback.increment();
                    // Cause class + tenant last-4 only (PII rule) — never the full tenant id or a body.
                    log.warn("tenant policy load failed ({}) — fail-open default for tenant …{}",
                            e.getClass().getSimpleName(), last4(tenantId));
                    return Mono.just(TenantPolicy.defaultFor(tenantId));
                });
    }

    /** 5xx → CB fault; everything else (2xx, 4xx incl. 409 + Retry-After) passes through unchanged. */
    private static Mono<ResponseEntity<String>> passThrough(ClientResponse response) {
        if (response.statusCode().is5xxServerError()) {
            int status = response.statusCode().value();
            return response.releaseBody().then(Mono.error(new UpstreamUnavailableException(status)));
        }
        return response.toEntity(String.class);
    }

    private static String last4(UUID id) {
        String s = id.toString();
        return s.substring(s.length() - 4);
    }
}
