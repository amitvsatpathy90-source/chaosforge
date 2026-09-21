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
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * MCP transport-proxy contract for {@link ControlPlaneClient#forwardMcp}.
 *
 * <p>The Gateway must remain protocol-blind: preserve the opaque request body and required MCP
 * transport headers, stream successful responses, pass through CP 4xx responses, and classify CP
 * 5xx responses through the existing {@link UpstreamUnavailableException} path.
 */
class ControlPlaneClientMcpTest {

    private static final Duration AWAIT = Duration.ofSeconds(5);
    private static final String MCP_PROTOCOL_VERSION = "MCP-Protocol-Version";

    private HttpServer server;
    private ControlPlaneClient client;

    private final AtomicReference<String> authorization = new AtomicReference<>();
    private final AtomicReference<String> contentType = new AtomicReference<>();
    private final AtomicReference<String> accept = new AtomicReference<>();
    private final AtomicReference<String> protocolVersion = new AtomicReference<>();
    private final AtomicReference<byte[]> requestBody = new AtomicReference<>();

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.start();

        client = new ControlPlaneClient(
                WebClient.builder()
                        .baseUrl("http://localhost:" + server.getAddress().getPort())
                        .build(),
                CircuitBreakerRegistry.ofDefaults(),
                BulkheadRegistry.ofDefaults(),
                new SimpleMeterRegistry());
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    /**
     * The load-bearing transport case: request bytes and required MCP headers reach CP unchanged,
     * while the successful response remains a {@code DataBuffer} stream.
     */
    @Test
    void forwardsBodyAndTransportHeaders_andStreamsResponse() {
        byte[] request = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\"}".getBytes(UTF_8);
        byte[] response = "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}".getBytes(UTF_8);

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
                        Flux.just(DefaultDataBufferFactory.sharedInstance.wrap("{}".getBytes(UTF_8))),
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
     * CP 5xx is the existing Gateway upstream-failure contract and must not be exposed as a raw CP response.
     */
    @Test
    void fiveHundredResponse_mapsToUpstreamUnavailable() {
        server.createContext("/mcp", exchange -> {
            byte[] response = "control plane unavailable".getBytes(UTF_8);
            exchange.sendResponseHeaders(503, response.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(response);
            }
        });

        assertThatThrownBy(() -> client.forwardMcp(
                        Flux.just(DefaultDataBufferFactory.sharedInstance.wrap("{}".getBytes(UTF_8))),
                        "Bearer mcp-token",
                        "application/json",
                        "application/json, text/event-stream",
                        null)
                .block(AWAIT))
                .isInstanceOf(UpstreamUnavailableException.class)
                .hasMessage("control plane returned 503");
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
}
