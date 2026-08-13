package io.chaosforge.execution.config;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Fail-fast + observable guard for this service's security posture (arch-audit F2).
 * Mirrors the CP guard over exec's own control set (mTLS, CP client leg, SSRF allowlist).
 * Fail-fast throws during bean init, before the port opens; the gauge covers a forgotten marker too.
 */
@Configuration
public class DeploymentPostureGuard {

    private static final Logger log = LoggerFactory.getLogger(DeploymentPostureGuard.class);
    private static final String DEPLOYED = "deployed";

    private final boolean hardened;

    public DeploymentPostureGuard(
            @Value("${chaosforge.deployment:lab}") String deployment,
            @Value("${server.ssl.bundle:}") String sslBundle,
            @Value("${server.ssl.client-auth:}") String clientAuth,
            @Value("${chaosforge.mtls.client-bundle:}") String clientBundle,
            @Value("${chaosforge.control-plane.base-url:}") String controlPlaneBaseUrl,
            @Value("${chaosforge.target.allowed-hosts:}") String allowedHosts) {

        List<String> gaps = new ArrayList<>();
        if (sslBundle.isBlank()) {
            gaps.add("server.ssl.bundle unset — the management/admin HTTP surface is plain HTTP, so the "
                    + "OPERATOR-gated kill switch is exposed without mTLS (mtls-rules.md)");
        }
        if (!"need".equalsIgnoreCase(clientAuth.trim())) {
            gaps.add("server.ssl.client-auth != need — a client cert is not mandatory ('want' is "
                    + "explicitly forbidden by mtls-rules.md)");
        }
        if (clientBundle.isBlank()) {
            gaps.add("chaosforge.mtls.client-bundle unset — the exec->CP rule-set fetch presents no client "
                    + "cert (HttpClientConfig attaches the bundle only when this has text)");
        }
        if (!controlPlaneBaseUrl.trim().toLowerCase(Locale.ROOT).startsWith("https://")) {
            gaps.add("chaosforge.control-plane.base-url is not https:// — the exec->CP leg is plaintext, "
                    + "carrying the forwarded tenant JWT in the clear");
        }
        if (allowedHosts.isBlank()) {
            // Empty allowlist means open mode; block-private-networks alone is not the ceiling.
            gaps.add("chaosforge.target.allowed-hosts empty — TargetUrlGuard is in open mode (any public "
                    + "host passes) on the service that actually fires the faults; open mode is documented "
                    + "as not DNS-rebinding-proof (ADR-0534)");
        }
        this.hardened = gaps.isEmpty();

        if (DEPLOYED.equalsIgnoreCase(deployment.trim()) && !hardened) {
            throw new IllegalStateException(
                    "chaosforge.deployment=deployed, but the security posture is incomplete: "
                    + String.join(" | ", gaps)
                    + " -- Start with --spring.profiles.active=mtls (application-mtls.yml supplies all of "
                    + "these) and pin TARGET_ALLOWED_HOSTS, or set chaosforge.deployment=lab if this really "
                    + "is a lab run.");
        }
        if (!hardened) {
            log.warn("SECURITY POSTURE: lab-only, NOT hardened (deployment={}): {}", deployment,
                    String.join(" | ", gaps));
        }
    }

    /** 1 = deployed posture fully on; 0 = at least one control off. Emitted unconditionally. */
    @Bean
    public MeterBinder securityPostureMeters() {
        return registry -> Gauge.builder("chaosforge.security.hardened", () -> hardened ? 1.0 : 0.0)
                .description("1 = deployed security posture fully on (mTLS client-auth=need, client cert to "
                        + "CP over https, SSRF allowlist); 0 = at least one control off — see startup WARN")
                .register(registry);
    }
}
