package io.chaosforge.execution.steadystate;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import io.chaosforge.common.target.TargetUrlGuard;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

/**
 * The HTTP target-health probe (acceptance gate C20) against a real local server — proving it is wired
 * to actual target responses: 2xx is healthy; a 5xx, a 4xx, and a refused connection are all unhealthy.
 */
class HttpTargetHealthProbeTest {

    private static HttpServer server;
    private static String base;

    @BeforeAll
    static void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/health", ex -> respond(ex, 200));     // healthy
        server.createContext("/sick", ex -> respond(ex, 503));       // degraded
        server.createContext("/missing", ex -> respond(ex, 404));    // not found
        server.start();
        base = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterAll
    static void stop() {
        server.stop(0);
    }

    // The reachability tests hit 127.0.0.1, so this probe's guard must allow loopback
    // (blockPrivateNetworks=false) — otherwise every probe would be rejected before the request.
    private final TargetHealthProbe probe =
            new HttpTargetHealthProbe(RestClient.create(), new TargetUrlGuard(false, List.of()));

    @Test
    void http200_isHealthy() {
        assertThat(probe.isHealthy(base + "/health")).isTrue();
    }

    @Test
    void guardBlockedTarget_isUnhealthy_withoutMakingTheRequest() {
        // Same SSRF boundary as the step path (E3/ADR-0534): a probe whose guard rejects the URL
        // reports unhealthy, and never reaches out. Here the guard blocks private networks, so the
        // loopback server above — reachable and returning 200 — is still reported unhealthy.
        TargetHealthProbe guarded =
                new HttpTargetHealthProbe(RestClient.create(), new TargetUrlGuard(true, List.of()));
        assertThat(guarded.isHealthy(base + "/health")).isFalse();
    }

    @Test
    void http503_isUnhealthy() {
        assertThat(probe.isHealthy(base + "/sick")).isFalse();
    }

    @Test
    void http404_isUnhealthy() {
        assertThat(probe.isHealthy(base + "/missing")).isFalse();
    }

    @Test
    void refusedConnection_isUnhealthy_notAnException() {
        // Nothing is listening on this port → the probe swallows the error and reports unhealthy.
        assertThat(probe.isHealthy("http://127.0.0.1:1/health")).isFalse();
    }

    private static void respond(com.sun.net.httpserver.HttpExchange ex, int status) throws IOException {
        ex.sendResponseHeaders(status, -1);
        ex.close();
    }
}
