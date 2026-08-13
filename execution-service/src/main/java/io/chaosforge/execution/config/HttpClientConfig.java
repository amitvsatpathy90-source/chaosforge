package io.chaosforge.execution.config;

import java.time.Duration;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.http.client.ClientHttpRequestFactoryBuilder;
import org.springframework.boot.http.client.HttpClientSettings;
import org.springframework.boot.http.client.HttpRedirects;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestClient;

/** Blocking RestClients (MVC + virtual threads). Per-call timeouts bound each outbound request. */
@Configuration
public class HttpClientConfig {

    /**
     * Calls to arbitrary tenant-supplied scenario targets — deliberately NOT mTLS-wired. These are
     * untrusted external endpoints; the internal CA is irrelevant to them. The read timeout is generous
     * (30s) because a fault step may legitimately exercise a slow target.
     *
     * <p><b>DONT_FOLLOW is a security control, not a preference (SEC-01).</b> {@code TargetUrlGuard}
     * validates only the <em>authored</em> URL, once, at rule-set cache-load ({@code CpRuleSetLoader}).
     * A 3xx {@code Location} is a URL the guard never saw — an open redirect on an allowlisted SUT, or an
     * attacker 302 to {@code 169.254.169.254} / an RFC1918 host, would be a silent SSRF bypass of the
     * blast-radius ceiling if followed. Suppress redirects so a 3xx surfaces as its status
     * ({@code < 500 ⇒ step COMPLETED}), never a second unvalidated hop.
     */
    @Bean
    public RestClient targetRestClient(
            @Value("${chaosforge.step.connect-timeout-ms:2000}") int connectMs,
            @Value("${chaosforge.step.read-timeout-ms:30000}") int readMs) {
        HttpClientSettings settings = HttpClientSettings.defaults()
                .withRedirects(HttpRedirects.DONT_FOLLOW)
                .withConnectTimeout(Duration.ofMillis(connectMs))
                .withReadTimeout(Duration.ofMillis(readMs));
        ClientHttpRequestFactory factory = ClientHttpRequestFactoryBuilder.detect().build(settings);
        return RestClient.builder().requestFactory(factory).build();
    }

    /**
     * Steady-state health probes (arch-audit M1). SEPARATE from {@link #targetRestClient} with a SHORT
     * read timeout: a health check must resolve fast, and the probe loop runs inside the aggregate step
     * deadline. Reusing the 30s step read-timeout meant a connected-but-slow target could make a single
     * probe hang 30s, and N distinct hosts × 3 breach probes could overshoot the aggregate deadline far
     * enough to breach {@code max.poll.interval.ms} → a self-inflicted consumer rebalance. Default 2s.
     *
     * <p>Redirects are suppressed for the same SSRF reason as {@link #targetRestClient} (SEC-01): the
     * probe URL is derived from the step authority, so a 3xx would escape the guard the same way.
     */
    @Bean
    public RestClient probeRestClient(
            @Value("${chaosforge.steady-state.probe-connect-timeout-ms:1000}") int connectMs,
            @Value("${chaosforge.steady-state.probe-read-timeout-ms:2000}") int readMs) {
        HttpClientSettings settings = HttpClientSettings.defaults()
                .withRedirects(HttpRedirects.DONT_FOLLOW)
                .withConnectTimeout(Duration.ofMillis(connectMs))
                .withReadTimeout(Duration.ofMillis(readMs));
        ClientHttpRequestFactory factory = ClientHttpRequestFactoryBuilder.detect().build(settings);
        return RestClient.builder().requestFactory(factory).build();
    }

    /**
     * Rule-set fetch to the Control Plane. Under the {@code mtls} profile
     * ({@code chaosforge.mtls.client-bundle=internal-mtls}) the request factory is built from the
     * internal-CA {@link SslBundles bundle}, so the call presents a client certificate (ADR-0531/0524).
     * Absent the property the call is plain HTTP (dev/tests). A configured-but-missing bundle throws at
     * startup — no silent downgrade to an unauthenticated channel.
     */
    @Bean
    public RestClient controlPlaneRestClient(
            @Value("${chaosforge.control-plane.base-url}") String baseUrl,
            @Value("${chaosforge.mtls.client-bundle:}") String clientBundle,
            ObjectProvider<SslBundles> sslBundles) {
        HttpClientSettings settings = HttpClientSettings.defaults()
                .withConnectTimeout(Duration.ofSeconds(2))
                .withReadTimeout(Duration.ofSeconds(5));
        if (StringUtils.hasText(clientBundle)) {
            settings = settings.withSslBundle(sslBundles.getObject().getBundle(clientBundle));
        }
        ClientHttpRequestFactory factory = ClientHttpRequestFactoryBuilder.detect().build(settings);
        return RestClient.builder().baseUrl(baseUrl).requestFactory(factory).build();
    }
}
