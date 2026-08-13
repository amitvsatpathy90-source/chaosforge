package io.chaosforge.controlplane.config;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Fail-fast + observable guard for this service's security posture (arch-audit F2, ADR-0541).
 * Asserts each control by its own property, not by profile name — see mtls-rules.md / ADR-0532.
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
            @Value("${chaosforge.mtls.internal-peer-cn:}") String internalPeerCn,
            @Value("${chaosforge.target.allowed-hosts:}") String allowedHosts) {

        List<String> gaps = new ArrayList<>();
        if (sslBundle.isBlank()) {
            gaps.add("server.ssl.bundle unset — plain HTTP, no mTLS (mtls-rules.md)");
        }
        if (!"need".equalsIgnoreCase(clientAuth.trim())) {
            gaps.add("server.ssl.client-auth != need — a client cert is not mandatory ('want' is "
                    + "explicitly forbidden by mtls-rules.md)");
        }
        if (internalPeerCn.isBlank()) {
            gaps.add("chaosforge.mtls.internal-peer-cn unset — /internal is permitAll, so its "
                    + "peer-asserted tenantId is unverified: any reachable caller can read any tenant's "
                    + "rule-set (ADR-0532)");
        }
        if (allowedHosts.isBlank()) {
            // Empty allowlist means open mode; block-private-networks alone is not the ceiling.
            gaps.add("chaosforge.target.allowed-hosts empty — TargetUrlGuard is in open mode (any public "
                    + "host passes), which target-validation-rules.md documents as not DNS-rebinding-proof");
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
                .description("1 = deployed security posture fully on (mTLS client-auth=need, /internal CN "
                        + "scoping, SSRF allowlist); 0 = at least one control off — see the startup WARN")
                .register(registry);
    }
}
