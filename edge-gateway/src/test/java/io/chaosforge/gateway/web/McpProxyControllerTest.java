package io.chaosforge.gateway.web;

import com.sun.net.httpserver.HttpServer;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.chaosforge.gateway.client.ControlPlaneClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

/** CP's stateless MCP transport 400s without both media types; the proxy must default a missing Accept. */
class McpProxyControllerTest {

    private static final Duration AWAIT = Duration.ofSeconds(5);
    private static final String DEFAULT_ACCEPT = "application/json, text/event-stream";

    private HttpServer server;
    private McpProxyController controller;
    private final AtomicReference<String> receivedAccept = new AtomicReference<>();

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/mcp", exchange -> {
            receivedAccept.set(exchange.getRequestHeaders().getFirst(HttpHeaders.ACCEPT));
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(200, 2);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write("{}".getBytes(UTF_8));
            }
        });
        server.start();
        controller = new McpProxyController(new ControlPlaneClient(
                WebClient.builder().baseUrl("http://localhost:" + server.getAddress().getPort()).build(),
                CircuitBreakerRegistry.ofDefaults(), BulkheadRegistry.ofDefaults(), new SimpleMeterRegistry()));
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private static Flux<DataBuffer> body() {
        return Flux.just(DefaultDataBufferFactory.sharedInstance.wrap("{}".getBytes(UTF_8)));
    }

    @Test
    void missingAccept_isDefaultedToBothMcpMediaTypes() {
        controller.forward(body(), "Bearer t", "application/json", null, null).block(AWAIT);

        assertThat(receivedAccept.get()).isEqualTo(DEFAULT_ACCEPT);
    }
}
