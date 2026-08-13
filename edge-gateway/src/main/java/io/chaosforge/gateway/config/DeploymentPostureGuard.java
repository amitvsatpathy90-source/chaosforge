package io.chaosforge.gateway.config;

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
 * Fail-fast + observable guard for the gateway's security posture (arch-audit F2); counterpart to
 * the CP/exec guards but scoped to a much smaller control set. Does NOT assert server-side TLS —
 * the public listener is HTTP by design here, terminating upstream (ADR-0531). Instead asserts the
 * outbound leg to the CP: {@code chaosforge.mtls.client-bundle} (client cert presented) and
 * {@code chaosforge.control-plane.base-url} (must be https — else the forwarded tenant JWT is
 * plaintext on the wire). Checks each property directly, not the profile name, same as the other guards.
 */
@Configuration
public class DeploymentPostureGuard {

    private static final Logger log = LoggerFactory.getLogger(DeploymentPostureGuard.class);
    private static final String DEPLOYED = "deployed";

    private final boolean hardened;

    public DeploymentPostureGuard(
            @Value("${chaosforge.deployment:lab}") String deployment,
            @Value("${chaosforge.mtls.client-bundle:}") String clientBundle,
            @Value("${chaosforge.control-plane.base-url:}") String controlPlaneBaseUrl) {

        List<String> gaps = new ArrayList<>();
        if (clientBundle.isBlank()) {
            gaps.add("chaosforge.mtls.client-bundle unset — the gateway->CP calls present no client cert; "
                    + "against an unhardened CP that silently succeeds unauthenticated");
        }
        if (!controlPlaneBaseUrl.trim().toLowerCase(Locale.ROOT).startsWith("https://")) {
            gaps.add("chaosforge.control-plane.base-url is not https:// — the gateway->CP leg is plaintext, "
                    + "carrying the forwarded tenant JWT in the clear");
        }
        this.hardened = gaps.isEmpty();

        if (DEPLOYED.equalsIgnoreCase(deployment.trim()) && !hardened) {
            throw new IllegalStateException(
                    "chaosforge.deployment=deployed, but the security posture is incomplete: "
                    + String.join(" | ", gaps)
                    + " -- Start with --spring.profiles.active=mtls (application-mtls.yml supplies both), or "
                    + "set chaosforge.deployment=lab if this really is a lab run. NOTE: this does not cover "
                    + "the PUBLIC listener, which is HTTP by design here (TLS terminates upstream).");
        }
        if (!hardened) {
            log.warn("SECURITY POSTURE: lab-only, NOT hardened (deployment={}): {}", deployment,
                    String.join(" | ", gaps));
        }
    }

    /** 1 = gateway->CP leg hardened (client cert, https); 0 = at least one off. Excludes the public listener. */
    @Bean
    public MeterBinder securityPostureMeters() {
        return registry -> Gauge.builder("chaosforge.security.hardened", () -> hardened ? 1.0 : 0.0)
                .description("1 = gateway->CP leg hardened (internal-CA client cert over https); 0 = at "
                        + "least one control off — see the startup WARN. Excludes the public listener.")
                .register(registry);
    }
}
