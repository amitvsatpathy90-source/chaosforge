package io.chaosforge.execution.ruleset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.chaosforge.common.target.TargetUrlGuard;
import io.chaosforge.execution.dlq.DlqRoutableException;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

/**
 * Proves the SSRF/blast-radius guard (ADR-0534) actually fires on {@link CpRuleSetLoader#loadPinned}
 * — the enforcement point ADR-0534 calls "the hard control", since authoring's 422 only covers rule
 * sets created through the ordinary API, not one written straight to the DB or persisted before the
 * guard existed. {@code TargetUrlGuardTest} exercises the guard class in isolation; nothing previously
 * proved this load path wires it in, or that a rejection is a terminal {@code STEP_FAILED} rather than
 * the replayable {@code INFRA_TRANSIENT} the CB-wrapped fetch produces for every other failure here.
 */
class CpRuleSetLoaderTest {

    private static HttpServer server;
    private static String base;
    private static volatile String stepsJson;

    @BeforeAll
    static void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/internal/rule-sets", CpRuleSetLoaderTest::respondSteps);
        server.start();
        base = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterAll
    static void stop() {
        server.stop(0);
    }

    private static void respondSteps(HttpExchange ex) throws IOException {
        byte[] body = stepsJson.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.sendResponseHeaders(200, body.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(body);
        }
    }

    private static CpRuleSetLoader loaderWithGuard(TargetUrlGuard guard) {
        return new CpRuleSetLoader(RestClient.create(base), CircuitBreakerRegistry.ofDefaults(),
                BulkheadRegistry.ofDefaults(), guard);
    }

    @Test
    void loadPinned_stepTargetingMetadataHost_isTerminalStepFailed_notInfraTransient() {
        stepsJson = "[{\"stepId\":\"s1\",\"stepType\":\"LATENCY\","
                + "\"targetUrl\":\"http://169.254.169.254/latest/meta-data/\","
                + "\"method\":\"GET\",\"faultConfig\":{}}]";
        CpRuleSetLoader loader = loaderWithGuard(new TargetUrlGuard(true, List.of()));

        assertThatThrownBy(() -> loader.loadPinned(UUID.randomUUID(), 1, UUID.randomUUID()))
                .isInstanceOfSatisfying(DlqRoutableException.class, e -> {
                    assertThat(e.dlqReason()).isEqualTo("STEP_FAILED");
                    assertThat(e.replayable()).isFalse();
                });
    }

    @Test
    void loadPinned_stepTargetingAllowlistedHost_returnsRuleSetRef() {
        stepsJson = "[{\"stepId\":\"s1\",\"stepType\":\"LATENCY\","
                + "\"targetUrl\":\"" + base + "/inject\","
                + "\"method\":\"GET\",\"faultConfig\":{}}]";
        // Allowlist mode: 127.0.0.1 is explicitly sanctioned, overriding the private-network block —
        // the in-mesh-ingress case ADR-0534 documents (e.g. a private RPE ingress host).
        CpRuleSetLoader loader = loaderWithGuard(new TargetUrlGuard(true, List.of("127.0.0.1")));

        RuleSetRef ref = loader.loadPinned(UUID.randomUUID(), 1, UUID.randomUUID());

        assertThat(ref.steps()).hasSize(1);
        assertThat(ref.steps().get(0).targetUrl()).isEqualTo(base + "/inject");
    }
}
