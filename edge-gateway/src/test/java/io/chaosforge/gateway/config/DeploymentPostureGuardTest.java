package io.chaosforge.gateway.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

/**
 * Pins the gateway deployment-posture guard (arch-audit F2). Scope is the gateway's OUTBOUND leg to the CP;
 * the public listener is HTTP by design and deliberately unasserted — {@link
 * #guardIsScopedToTheOutboundLeg_notThePublicListener()} pins that so it is not "completed" later by
 * copying the CP/exec server-side checks over.
 */
class DeploymentPostureGuardTest {

    private static final String CLIENT_BUNDLE = "internal-mtls";
    private static final String CP_URL = "https://control-plane:8081";

    private static DeploymentPostureGuard guard(String deployment, String clientBundle, String cpUrl) {
        return new DeploymentPostureGuard(deployment, clientBundle, cpUrl);
    }

    private static double hardenedGauge(DeploymentPostureGuard guard) {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        guard.securityPostureMeters().bindTo(registry);
        return registry.get("chaosforge.security.hardened").gauge().value();
    }

    @Test
    void deployed_fullyHardened_startsAndReportsOne() {
        assertThat(hardenedGauge(guard("deployed", CLIENT_BUNDLE, CP_URL))).isEqualTo(1.0);
    }

    @Test
    void deployed_withoutClientBundle_refusesToStart() {
        assertThatThrownBy(() -> guard("deployed", "", CP_URL))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("client-bundle unset");
    }

    @Test
    void deployed_withPlaintextControlPlaneUrl_refusesToStart() {
        // The default is http://localhost:8081 — only the mtls profile flips it to https://.
        assertThatThrownBy(() -> guard("deployed", CLIENT_BUNDLE, "http://localhost:8081"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("base-url is not https://");
    }

    @Test
    void deployed_namesEveryMissingControl_notJustTheFirst() {
        assertThatThrownBy(() -> guard("deployed", "", "http://cp:8081"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("client-bundle unset")
                .hasMessageContaining("base-url is not https://");
    }

    @Test
    void guardIsScopedToTheOutboundLeg_notThePublicListener() {
        // The gateway's public listener is HTTP BY DESIGN (it would need a public CA cert; TLS terminates
        // upstream — ADR-0531, architecture specifications §Known Limitations). A fully-hardened verdict here must therefore
        // be reachable with NO server-side TLS configured at all. If someone later copies the CP/exec
        // server.ssl / client-auth=need checks into this guard, this test fails — which is the point:
        // client-auth=need on a public ingress would reject every real client.
        assertThat(hardenedGauge(guard("deployed", CLIENT_BUNDLE, CP_URL))).isEqualTo(1.0);
        assertThatCode(() -> guard("deployed", CLIENT_BUNDLE, CP_URL)).doesNotThrowAnyException();
    }

    @Test
    void httpsCheckIsCaseInsensitive_andToleratesWhitespace() {
        assertThatCode(() -> guard("deployed", CLIENT_BUNDLE, " HTTPS://cp:8081 ")).doesNotThrowAnyException();
    }

    @Test
    void httpsCheckIsNotASubstringMatch() {
        assertThatThrownBy(() -> guard("deployed", CLIENT_BUNDLE, "http://https.cp:8081"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("base-url is not https://");
    }

    @Test
    void lab_isThePermissiveDefault_andStartsUnhardened() {
        assertThatCode(() -> guard("lab", "", "http://localhost:8081")).doesNotThrowAnyException();
    }

    @Test
    void forgottenMarkerAndProfile_stillReportsZero() {
        // The residual the fail-fast cannot cover: a flag cannot protect against a forgotten flag.
        assertThat(hardenedGauge(guard("lab", "", "http://localhost:8081"))).isEqualTo(0.0);
    }

    @Test
    void controlsAssertedByProperty_notByProfileName() {
        assertThat(hardenedGauge(guard("lab", CLIENT_BUNDLE, CP_URL))).isEqualTo(1.0);
    }
}
