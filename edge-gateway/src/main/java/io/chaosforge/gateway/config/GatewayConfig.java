package io.chaosforge.gateway.config;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.http.client.HttpClientSettings;
import org.springframework.boot.http.client.reactive.ClientHttpConnectorBuilder;
import org.springframework.boot.ssl.SslBundle;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.http.client.reactive.ClientHttpConnector;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.util.StringUtils;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class GatewayConfig {

    /**
     * Outbound channel to the Control Plane. Under the {@code mtls} profile
     * ({@code chaosforge.mtls.client-bundle=internal-mtls}) the reactive (Netty) connector is built from
     * the internal-CA {@link SslBundle}, so every CP call presents a client certificate (ADR-0531/0524).
     * Absent the property the gateway talks plain HTTP (dev/tests). A configured-but-missing bundle throws
     * at startup — we never silently downgrade to an unauthenticated channel.
     */
    @Bean
    public WebClient controlPlaneWebClient(
            @Value("${chaosforge.control-plane.base-url}") String baseUrl,
            @Value("${chaosforge.mtls.client-bundle:}") String clientBundle,
            ObjectProvider<SslBundles> sslBundles,
            WebClient.Builder builder) {
        // The auto-configured Builder (vs WebClient.builder()) carries the ObservationRegistry, so CP
        // calls open a client observation and propagate W3C traceparent — CP's server observation joins
        // it, correlating gateway and CP log lines under one trace_id. Prototype-scoped: safe to mutate.
        builder.baseUrl(baseUrl);
        if (StringUtils.hasText(clientBundle)) {
            SslBundle bundle = sslBundles.getObject().getBundle(clientBundle);
            ClientHttpConnector connector = ClientHttpConnectorBuilder.reactor()
                    .build(HttpClientSettings.ofSslBundle(bundle));
            builder.clientConnector(connector);
        }
        return builder.build();
    }

    /** Atomic sliding-window rate-limit script — single shared Redis key per tenant (true global). */
    @Bean
    public RedisScript<Long> rateLimitScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(new ClassPathResource("scripts/sliding_window.lua")));
        script.setResultType(Long.class);
        return script;
    }
}
