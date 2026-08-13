package io.chaosforge.execution.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

/**
 * SEC-01 regression: the {@code target} and {@code probe} RestClients must NOT follow HTTP redirects.
 * {@code TargetUrlGuard} validates only the <em>authored</em> URL, once, at rule-set cache-load
 * ({@code CpRuleSetLoader}); a 3xx {@code Location} is a host the guard never vetted — an open redirect
 * on an allowlisted SUT, or an attacker 302 to {@code 169.254.169.254} / an RFC1918 address — so a
 * followed redirect is a silent SSRF bypass of the blast-radius ceiling. The 302 must surface as its
 * status, not a second hop.
 *
 * <p>The server's {@code Location} points at a local {@code 200} endpoint precisely so "followed" is
 * observable: no-follow ⇒ the client returns {@code 302}; follow ⇒ it would return {@code 200}.
 */
class HttpClientConfigRedirectTest {

    private static HttpServer server;
    private static String base;

    @BeforeAll
    static void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/redirect", ex -> {
            ex.getResponseHeaders().add("Location", base + "/followed");   // in the wild: an internal host
            ex.sendResponseHeaders(302, -1);
            ex.close();
        });
        server.createContext("/followed", ex -> respond(ex, 200));
        server.start();
        base = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterAll
    static void stop() {
        server.stop(0);
    }

    private final HttpClientConfig config = new HttpClientConfig();

    @Test
    void targetRestClient_doesNotFollowRedirects() {
        int status = statusOf(config.targetRestClient(2000, 30000), base + "/redirect");
        assertThat(status).as("target client must surface the 302, never follow to the 200").isEqualTo(302);
    }

    @Test
    void probeRestClient_doesNotFollowRedirects() {
        int status = statusOf(config.probeRestClient(1000, 2000), base + "/redirect");
        assertThat(status).as("probe client must surface the 302, never follow to the 200").isEqualTo(302);
    }

    /** Mirrors StepExecutor: read the raw status with no default 4xx/5xx throw. */
    private static int statusOf(RestClient client, String url) {
        return client.get().uri(url).exchange((request, response) -> response.getStatusCode().value());
    }

    private static void respond(HttpExchange ex, int status) throws IOException {
        ex.sendResponseHeaders(status, -1);
        ex.close();
    }
}
