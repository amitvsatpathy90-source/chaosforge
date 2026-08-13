package io.chaosforge.execution.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

/**
 * Pins the exec deployment-posture guard (arch-audit F2). The guard's whole value is that an insecure start
 * is <b>loud</b> — it either refuses to refresh the context or reports {@code hardened=0}. These assert both
 * halves, including the residual the fail-fast deliberately cannot cover (the marker itself forgotten).
 */
class DeploymentPostureGuardTest {

    private static final String BUNDLE = "internal-mtls";
    private static final String NEED = "need";
    private static final String CLIENT_BUNDLE = "internal-mtls";
    private static final String CP_URL = "https://control-plane:8081";
    private static final String HOSTS = "rpe.internal";

    private static DeploymentPostureGuard guard(String deployment, String bundle, String clientAuth,
            String clientBundle, String cpUrl, String allowedHosts) {
        return new DeploymentPostureGuard(deployment, bundle, clientAuth, clientBundle, cpUrl, allowedHosts);
    }

    private static double hardenedGauge(DeploymentPostureGuard guard) {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        guard.securityPostureMeters().bindTo(registry);
        return registry.get("chaosforge.security.hardened").gauge().value();
    }

    @Test
    void deployed_fullyHardened_startsAndReportsOne() {
        DeploymentPostureGuard g = guard("deployed", BUNDLE, NEED, CLIENT_BUNDLE, CP_URL, HOSTS);
        assertThat(hardenedGauge(g)).isEqualTo(1.0);
    }

    @Test
    void deployed_withoutTls_refusesToStart() {
        assertThatThrownBy(() -> guard("deployed", "", NEED, CLIENT_BUNDLE, CP_URL, HOSTS))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("server.ssl.bundle unset");
    }

    @Test
    void deployed_withClientAuthWant_refusesToStart() {
        assertThatThrownBy(() -> guard("deployed", BUNDLE, "want", CLIENT_BUNDLE, CP_URL, HOSTS))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("client-auth != need");
    }

    @Test
    void deployed_withoutClientBundle_refusesToStart() {
        // HttpClientConfig attaches the SSL bundle only when this has text — unset means the exec->CP
        // rule-set fetch presents no client cert at all.
        assertThatThrownBy(() -> guard("deployed", BUNDLE, NEED, "", CP_URL, HOSTS))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("client-bundle unset");
    }

    @Test
    void deployed_withPlaintextControlPlaneUrl_refusesToStart() {
        // The default is http://localhost:8081 — only the mtls profile flips it to https://. A deployed run
        // on the default would carry the forwarded tenant JWT in the clear.
        assertThatThrownBy(() -> guard("deployed", BUNDLE, NEED, CLIENT_BUNDLE, "http://localhost:8081", HOSTS))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("base-url is not https://");
    }

    @Test
    void deployed_withEmptyAllowlist_refusesToStart() {
        assertThatThrownBy(() -> guard("deployed", BUNDLE, NEED, CLIENT_BUNDLE, CP_URL, "  "))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("allowed-hosts empty");
    }

    @Test
    void deployed_namesEveryMissingControl_notJustTheFirst() {
        assertThatThrownBy(() -> guard("deployed", "", "", "", "http://cp:8081", ""))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("server.ssl.bundle unset")
                .hasMessageContaining("client-auth != need")
                .hasMessageContaining("client-bundle unset")
                .hasMessageContaining("base-url is not https://")
                .hasMessageContaining("allowed-hosts empty");
    }

    @Test
    void httpsCheckIsCaseInsensitive_andToleratesWhitespace() {
        assertThatCode(() -> guard("deployed", BUNDLE, NEED, CLIENT_BUNDLE, " HTTPS://cp:8081 ", HOSTS))
                .doesNotThrowAnyException();
    }

    @Test
    void httpsCheckIsNotASubstringMatch() {
        // A URL merely *containing* "https" must not pass — e.g. a plaintext host named for it.
        assertThatThrownBy(() -> guard("deployed", BUNDLE, NEED, CLIENT_BUNDLE, "http://https.cp:8081", HOSTS))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("base-url is not https://");
    }

    @Test
    void lab_isThePermissiveDefault_andStartsUnhardened() {
        // The default must stay permissive: no test in this repo uses @ActiveProfiles, so a fail-closed
        // default would fail the whole suite.
        assertThatCode(() -> guard("lab", "", "", "", "http://localhost:8081", ""))
                .doesNotThrowAnyException();
    }

    @Test
    void forgottenMarkerAndProfile_stillReportsZero() {
        // The residual the fail-fast cannot cover: a flag cannot protect against a forgotten flag.
        assertThat(hardenedGauge(guard("lab", "", "", "", "http://localhost:8081", ""))).isEqualTo(0.0);
    }

    @Test
    void controlsAssertedByProperty_notByProfileName() {
        // Regression pin, mirroring the CP guard: the posture is the properties, not the profile name.
        assertThat(hardenedGauge(guard("lab", BUNDLE, NEED, CLIENT_BUNDLE, CP_URL, HOSTS))).isEqualTo(1.0);
    }
}
