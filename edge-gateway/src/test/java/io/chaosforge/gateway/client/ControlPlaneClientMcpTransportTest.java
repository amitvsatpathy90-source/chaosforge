package io.chaosforge.gateway.client;

import com.sun.net.httpserver.HttpServer;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * MCP transport-proxy contract for {@link ControlPlaneClient#forwardMcp}: the Gateway must remain
 * protocol-blind — preserve the opaque request body and required MCP transport headers, stream
 * successful responses, pass through CP 4xx responses, and classify CP 5xx responses through
 * {@link UpstreamUnavailableException}.
 */
class ControlPlaneClientMcpTransportTest {

    private static final Duration AWAIT = Duration.ofSeconds(5);
    private static final String MCP_PROTOCOL_VERSION = "MCP-Protocol-Version";

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

    /** Consume and release the test response stream after asserting its bytes. */
    private static byte[] readBody(Flux<DataBuffer> body) {
        DataBuffer buffer = DataBufferUtils.join(body).block(AWAIT);
        assertThat(buffer).isNotNull();

        try {
            byte[] bytes = new byte[buffer.readableByteCount()];
            buffer.read(bytes);
            return bytes;
        } finally {
            DataBufferUtils.release(buffer);
        }
    }

    /**
     * The load-bearing transport case: request bytes and required MCP headers reach CP
     * unchanged, while the successful response remains a {@code DataBuffer} stream.
     */
    @Test
    void forwardsBodyAndTransportHeaders_andStreamsResponse() {
        byte[] request = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\"}".getBytes(UTF_8);
        byte[] response = "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}".getBytes(UTF_8);

        AtomicReference<String> authorization = new AtomicReference<>();
        AtomicReference<String> contentType = new AtomicReference<>();
        AtomicReference<String> accept = new AtomicReference<>();
        AtomicReference<String> protocolVersion = new AtomicReference<>();
        AtomicReference<byte[]> requestBody = new AtomicReference<>();

        server.createContext("/mcp", exchange -> {
            // Capture the opaque transport exactly as CP receives it.
            authorization.set(exchange.getRequestHeaders().getFirst(HttpHeaders.AUTHORIZATION));
            contentType.set(exchange.getRequestHeaders().getFirst(HttpHeaders.CONTENT_TYPE));
            accept.set(exchange.getRequestHeaders().getFirst(HttpHeaders.ACCEPT));
            protocolVersion.set(exchange.getRequestHeaders().getFirst(MCP_PROTOCOL_VERSION));
            requestBody.set(exchange.getRequestBody().readAllBytes());

            exchange.getResponseHeaders().set(HttpHeaders.CONTENT_TYPE, "application/json");
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(response);
            }
        });

        ResponseEntity<Flux<DataBuffer>> result = client.forwardMcp(
                        Flux.just(DefaultDataBufferFactory.sharedInstance.wrap(request)),
                        "Bearer mcp-token",
                        "application/json",
                        "application/json, text/event-stream",
                        "2025-11-25")
                .block(AWAIT);

        assertThat(result).isNotNull();
        assertThat(result.getStatusCode().value()).isEqualTo(200);
        assertThat(result.getHeaders().getFirst(HttpHeaders.CONTENT_TYPE))
                .isEqualTo("application/json");

        assertThat(authorization.get()).isEqualTo("Bearer mcp-token");
        assertThat(contentType.get()).isEqualTo("application/json");
        assertThat(accept.get()).isEqualTo("application/json, text/event-stream");
        assertThat(protocolVersion.get()).isEqualTo("2025-11-25");
        assertThat(requestBody.get()).isEqualTo(request);

        assertThat(readBody(result.getBody())).isEqualTo(response);
    }

    /**
     * CP 4xx responses are client-visible protocol/HTTP responses, not Gateway upstream failures.
     */
    @Test
    void fourHundredResponse_passesThroughUnchanged() {
        byte[] response = "{\"error\":\"unauthorized\"}".getBytes(UTF_8);

        server.createContext("/mcp", exchange -> {
            exchange.getResponseHeaders().set(HttpHeaders.CONTENT_TYPE, "application/json");
            exchange.sendResponseHeaders(401, response.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(response);
            }
        });

        ResponseEntity<Flux<DataBuffer>> result = client.forwardMcp(
                        emptyJsonBody(),
                        "Bearer mcp-token",
                        "application/json",
                        "application/json, text/event-stream",
                        null)
                .block(AWAIT);

        assertThat(result).isNotNull();
        assertThat(result.getStatusCode().value()).isEqualTo(401);
        assertThat(readBody(result.getBody())).isEqualTo(response);
    }

    /**
     * CP 5xx is the existing Gateway upstream-failure contract and must not be exposed as a
     * raw CP response.
     */
    @Test
    void fiveHundredResponse_mapsToUpstreamUnavailable() {
        respondWith(503, "control plane unavailable".getBytes(UTF_8));

        assertThatThrownBy(() -> forward(client))
                .isInstanceOf(UpstreamUnavailableException.class)
                .hasMessage("control plane returned 503");
    }
}
