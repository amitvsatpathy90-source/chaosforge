package io.chaosforge.controlplane.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

/**
 * Pins the deployment-posture guard (arch-audit F2). The guard's whole value is that an insecure start is
 * <b>loud</b> — it either refuses to refresh the context or reports {@code hardened=0}. These assert both
 * halves, including the residual the fail-fast deliberately cannot cover (the marker itself forgotten).
 */
class DeploymentPostureGuardTest {

    private static final String BUNDLE = "internal-mtls";
    private static final String NEED = "need";
    private static final String PEER_CN = "execution-service";
    private static final String HOSTS = "rpe.internal";

    /** A fully-hardened deployed posture; individual tests knock out one control at a time. */
    private static DeploymentPostureGuard guard(String deployment, String bundle, String clientAuth,
            String peerCn, String allowedHosts) {
        return new DeploymentPostureGuard(deployment, bundle, clientAuth, peerCn, allowedHosts);
    }

    private static double hardenedGauge(DeploymentPostureGuard guard) {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        guard.securityPostureMeters().bindTo(registry);
        return registry.get("chaosforge.security.hardened").gauge().value();
    }

    @Test
    void deployed_fullyHardened_startsAndReportsOne() {
        DeploymentPostureGuard g = guard("deployed", BUNDLE, NEED, PEER_CN, HOSTS);
        assertThat(hardenedGauge(g)).isEqualTo(1.0);
    }

    @Test
    void deployed_withoutTls_refusesToStart() {
        assertThatThrownBy(() -> guard("deployed", "", NEED, PEER_CN, HOSTS))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("server.ssl.bundle unset");
    }

    @Test
    void deployed_withClientAuthWant_refusesToStart() {
        // mtls-rules.md forbids 'want' outright — a client cert must be mandatory, not optional.
        assertThatThrownBy(() -> guard("deployed", BUNDLE, "want", PEER_CN, HOSTS))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("client-auth != need");
    }

    @Test
    void deployed_withoutInternalPeerCn_refusesToStart() {
        // The ADR-0532 hole: /internal falls back to permitAll and its tenantId is peer-asserted.
        assertThatThrownBy(() -> guard("deployed", BUNDLE, NEED, "", HOSTS))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("internal-peer-cn unset");
    }

    @Test
    void deployed_withEmptyAllowlist_refusesToStart() {
        assertThatThrownBy(() -> guard("deployed", BUNDLE, NEED, PEER_CN, "  "))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("allowed-hosts empty");
    }

    @Test
    void deployed_namesEveryMissingControl_notJustTheFirst() {
        // The message is the operator's whole diagnosis — it must not stop at the first gap.
        assertThatThrownBy(() -> guard("deployed", "", "", "", ""))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("server.ssl.bundle unset")
                .hasMessageContaining("client-auth != need")
                .hasMessageContaining("internal-peer-cn unset")
                .hasMessageContaining("allowed-hosts empty");
    }

    @Test
    void lab_isThePermissiveDefault_andStartsUnhardened() {
        // The default must stay permissive: no test in this repo uses @ActiveProfiles, so a fail-closed
        // default would fail the whole suite.
        assertThatCode(() -> guard("lab", "", "", "", "")).doesNotThrowAnyException();
    }

    @Test
    void forgottenMarkerAndProfile_stillReportsZero() {
        // The residual the fail-fast cannot cover: a flag cannot protect against a forgotten flag. The
        // gauge is emitted unconditionally, so this run is still alertable (hardened == 0).
        assertThat(hardenedGauge(guard("lab", "", "", "", ""))).isEqualTo(0.0);
    }

    @Test
    void controlsAssertedByProperty_notByProfileName() {
        // Regression pin. An earlier draft checked Environment.acceptsProfiles("mtls"), but the controls
        // key off their own properties: InternalMtlsSecurityConfig is @ConditionalOnProperty(
        // chaosforge.mtls.internal-peer-cn), and InternalMtlsAuthorizationIT activates it by property with
        // NO profile active. A profile check called that run unhardened (false negative) and would call a
        // profile-on/property-lost run hardened (false positive). No profile is involved here at all.
        assertThat(hardenedGauge(guard("lab", BUNDLE, NEED, PEER_CN, HOSTS))).isEqualTo(1.0);
    }

    @Test
    void markerIsCaseAndWhitespaceInsensitive() {
        assertThatThrownBy(() -> guard(" Deployed ", "", NEED, PEER_CN, HOSTS))
                .isInstanceOf(IllegalStateException.class);
    }
}
