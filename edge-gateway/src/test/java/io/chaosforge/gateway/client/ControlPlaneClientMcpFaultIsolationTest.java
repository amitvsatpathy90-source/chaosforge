package io.chaosforge.gateway.client;

import com.sun.net.httpserver.HttpServer;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Fault-isolation contract: gateway-mcp's CB, bulkhead, and timeout are separate instances from
 * gateway-cp's — an MCP-side CP overload must not trip the /v1 proxy's breaker, and vice versa.
 */
class ControlPlaneClientMcpFaultIsolationTest {

    private static final Duration AWAIT = Duration.ofSeconds(5);

    private HttpServer server;
    private ControlPlaneClient client;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.start();
        client = clientWith(CircuitBreakerRegistry.ofDefaults(), BulkheadRegistry.ofDefaults());
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    /** Builds a client against the running test server with the given CB/bulkhead config. */
    private ControlPlaneClient clientWith(CircuitBreakerRegistry cbRegistry, BulkheadRegistry bulkheadRegistry) {
        return new ControlPlaneClient(
                WebClient.builder().baseUrl("http://localhost:" + server.getAddress().getPort()).build(),
                cbRegistry, bulkheadRegistry, new SimpleMeterRegistry());
    }

    /** A fresh empty-JSON request body — a Flux can only be subscribed once, so no shared field. */
    private static Flux<DataBuffer> emptyJsonBody() {
        return Flux.just(DefaultDataBufferFactory.sharedInstance.wrap("{}".getBytes(UTF_8)));
    }

    /** Calls /mcp with a fixed bearer token + content type; no Accept/protocol-version. */
    private static ResponseEntity<Flux<DataBuffer>> forward(ControlPlaneClient target, Duration blockFor) {
        return target.forwardMcp(emptyJsonBody(), "Bearer mcp-token", "application/json", null, null)
                .block(blockFor);
    }

    private static ResponseEntity<Flux<DataBuffer>> forward(ControlPlaneClient target) {
        return forward(target, AWAIT);
    }

    /** Wires /mcp to always answer with a fixed status + body; returns a call counter. */
    private AtomicInteger respondWith(int status, byte[] body) {
        AtomicInteger calls = new AtomicInteger();
        server.createContext("/mcp", exchange -> {
            calls.incrementAndGet();
            exchange.sendResponseHeaders(status, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        return calls;
    }

    /**
     * gateway-mcp CB is isolated from gateway-cp — proven with a small custom window (real
     * config is 20-call/50%; too slow for a unit test): the call that trips it is the last
     * one that actually reaches CP.
     */
    @Test
    void circuitBreaker_opensAfterThreshold_andShortCircuitsWithoutCallingCp() {
        CircuitBreakerConfig smallWindow = CircuitBreakerConfig.custom()
                .slidingWindowSize(4)
                .minimumNumberOfCalls(4)
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofMinutes(1))
                .build();
        ControlPlaneClient mcpClient =
                clientWith(CircuitBreakerRegistry.of(smallWindow), BulkheadRegistry.ofDefaults());

        AtomicInteger callsReachingCp = respondWith(503, "unavailable".getBytes(UTF_8));

        // 4 failing calls fill the window and trip the breaker.
        for (int i = 0; i < 4; i++) {
            assertThatThrownBy(() -> forward(mcpClient)).isInstanceOf(UpstreamUnavailableException.class);
        }
        assertThat(callsReachingCp.get()).isEqualTo(4);

        // Breaker OPEN — next call short-circuits; CP call count must not move.
        assertThatThrownBy(() -> forward(mcpClient)).isInstanceOf(CallNotPermittedException.class);
        assertThat(callsReachingCp.get()).isEqualTo(4);
    }

    /**
     * gateway-mcp bulkhead is a separate permit pool from gateway-cp's. maxConcurrentCalls(1)
     * + fail-fast (no queueing) mirrors the real config's shape; a held first call proves the
     * second is rejected immediately rather than queued.
     */
    @Test
    void bulkhead_rejectsBeyondConcurrencyLimit_independentlyOfGatewayCp() throws InterruptedException {
        BulkheadConfig onePermit = BulkheadConfig.custom()
                .maxConcurrentCalls(1)
                .maxWaitDuration(Duration.ZERO)
                .build();
        ControlPlaneClient mcpClient =
                clientWith(CircuitBreakerRegistry.ofDefaults(), BulkheadRegistry.of(onePermit));

        CountDownLatch requestReceived = new CountDownLatch(1);
        CountDownLatch releaseResponse = new CountDownLatch(1);
        server.createContext("/mcp", exchange -> {
            requestReceived.countDown();
            await(releaseResponse);
            byte[] response = "{}".getBytes(UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(response);
            }
        });

        // Occupy the only permit with a call held open server-side.
        Mono<ResponseEntity<Flux<DataBuffer>>> inFlight =
                mcpClient.forwardMcp(emptyJsonBody(), "Bearer mcp-token", "application/json", null, null)
                        .subscribeOn(Schedulers.boundedElastic());
        inFlight.subscribe();
        requestReceived.await(AWAIT.toSeconds(), TimeUnit.SECONDS);

        // Second call: permit pool full, fail-fast — no queueing.
        assertThatThrownBy(() -> forward(mcpClient)).isInstanceOf(BulkheadFullException.class);

        releaseResponse.countDown();
    }

    /**
     * MCP_TIMEOUT (15s) is not the 3s TIMEOUT used by initiateReplay/getScenario — a 4s CP
     * delay, which would trip the 3s proxy timeout, must still succeed on the MCP path.
     */
    @Test
    void timeout_isLongerThanProxyTimeout_toleratesA4SecondCpDelay() {
        server.createContext("/mcp", exchange -> {
            sleep(Duration.ofSeconds(4));
            byte[] response = "{}".getBytes(UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(response);
            }
        });

        // > 4s CP delay + margin; local override, not AWAIT (which is 5s and too tight).
        ResponseEntity<Flux<DataBuffer>> result = forward(client, Duration.ofSeconds(6));

        assertThat(result).isNotNull();
        assertThat(result.getStatusCode().value()).isEqualTo(200);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void sleep(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
