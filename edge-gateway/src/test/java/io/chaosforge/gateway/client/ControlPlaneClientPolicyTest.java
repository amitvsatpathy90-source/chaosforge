package io.chaosforge.gateway.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import io.chaosforge.gateway.cache.TenantPolicy;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * The tenant-policy cache loader (arch-audit H1). Before the fix this fetch hit the JWT-guarded
 * {@code /v1/tenants/{id}} with <b>no</b> credential — every load 401'd and the silent
 * {@code onErrorReturn} handed every tenant the default policy, so per-tenant rate limits never
 * actually applied. These tests pin the corrected contract against a real HTTP server:
 *
 * <ul>
 *   <li>the loader calls the peer-authenticated <b>internal</b> path {@code /internal/tenants/{id}/policy}
 *       (process identity, not a user JWT — the cache refreshes outside any request context);</li>
 *   <li>a healthy response yields the CP-configured policy (not the default) + a success counter;</li>
 *   <li>failure stays fail-open (default policy; architecture specifications posture) but is now <b>observable</b> —
 *       the {@code chaosforge.gateway.policy_load{outcome="fallback"}} counter increments. The 401
 *       case is the exact regression that hid the original defect.</li>
 * </ul>
 */
class ControlPlaneClientPolicyTest {

    private static final Duration AWAIT = Duration.ofSeconds(5);

    private HttpServer server;
    private ControlPlaneClient client;
    private SimpleMeterRegistry registry;
    private final AtomicReference<String> requestedPath = new AtomicReference<>();

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.start();
        registry = new SimpleMeterRegistry();
        client = new ControlPlaneClient(
                WebClient.builder().baseUrl("http://localhost:" + server.getAddress().getPort()).build(),
                CircuitBreakerRegistry.ofDefaults(), BulkheadRegistry.ofDefaults(), registry);
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    @Test
    void fetchesPolicyFromInternalPath_withConfiguredLimit() {
        UUID tenantId = UUID.randomUUID();
        respondWith(200, "{\"tenantId\":\"" + tenantId + "\",\"rateLimitPerMin\":42}");

        TenantPolicy policy = client.fetchTenantPolicy(tenantId).block(AWAIT);

        assertThat(requestedPath.get())
                .as("must hit the peer-authenticated internal path, not the JWT-guarded /v1 API")
                .isEqualTo("/internal/tenants/" + tenantId + "/policy");
        assertThat(policy).isEqualTo(new TenantPolicy(tenantId, 42));
        assertThat(counter("success")).isEqualTo(1.0);
        assertThat(counter("fallback")).isZero();
    }

    @Test
    void unauthorizedResponse_failsOpenToDefault_andCountsFallback() {
        // The pre-fix failure mode: an auth rejection must never silently become "the default policy".
        UUID tenantId = UUID.randomUUID();
        respondWith(401, "");

        TenantPolicy policy = client.fetchTenantPolicy(tenantId).block(AWAIT);

        assertThat(policy).isEqualTo(TenantPolicy.defaultFor(tenantId));
        assertThat(counter("fallback")).isEqualTo(1.0);
        assertThat(counter("success")).isZero();
    }

    @Test
    void serverError_failsOpenToDefault_andCountsFallback() {
        UUID tenantId = UUID.randomUUID();
        respondWith(500, "");

        TenantPolicy policy = client.fetchTenantPolicy(tenantId).block(AWAIT);

        assertThat(policy).isEqualTo(TenantPolicy.defaultFor(tenantId));
        assertThat(counter("fallback")).isEqualTo(1.0);
    }

    private void respondWith(int status, String json) {
        server.createContext("/", exchange -> {
            requestedPath.set(exchange.getRequestURI().getPath());
            byte[] body = json.getBytes(UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
            if (body.length > 0) {
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(body);
                }
            } else {
                exchange.close();
            }
        });
    }

    private double counter(String outcome) {
        return registry.counter("chaosforge.gateway.policy_load", "outcome", outcome).count();
    }
}
