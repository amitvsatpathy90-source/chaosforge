package io.chaosforge.controlplane.it;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.x509;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

import java.io.InputStream;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.context.WebApplicationContext;

/**
 * ADR-0532 — the CP {@code /internal} surface is restricted to the Execution Service's client-cert
 * subject. mTLS ({@code client-auth: need}) authenticates <em>a</em> valid internal-CA peer; it does not
 * bind the {@code tenantId} that peer supplies. Since {@code InternalRuleSetController} scopes its read
 * by a caller-supplied {@code tenantId} param, without this control any internal cert holder (notably
 * the gateway) could read <em>any</em> tenant's rule-set by passing an arbitrary {@code tenantId}
 * (arch-audit 2.1).
 *
 * <p>Activating {@code chaosforge.mtls.internal-peer-cn} registers {@link
 * io.chaosforge.controlplane.config.InternalMtlsSecurityConfig} (mirrors the {@code mtls} profile). The
 * {@code x509()} post-processor injects the client cert exactly as the servlet container would after a
 * real mTLS handshake, so the X.509 authorization decision is exercised without standing up TLS.
 *
 * <p>Authorization is <b>per path</b>: each internal read is bound to the one peer allowed to call it,
 * and an unmapped {@code /internal} path is denyAll (fail-closed).
 *
 * <ul>
 *   <li>{@code /internal/rule-sets/**}: exec cert (CN=execution-service) → 200; gateway cert, a
 *       <em>valid</em> internal peer → 403 (the audit's fix); no cert → 403;</li>
 *   <li>{@code /internal/tenants/{id}/policy}: gateway cert → 200 (the gateway's rate-limit policy
 *       loader — arch-audit H1); exec cert → 403; no cert → 403.</li>
 * </ul>
 */
@TestPropertySource(properties = {
        "chaosforge.mtls.internal-peer-cn=execution-service",
        "chaosforge.mtls.internal-gateway-cn=edge-gateway"})
class InternalMtlsAuthorizationIT extends AbstractCpIntegrationTest {

    @MockitoBean
    private JwtDecoder jwtDecoder;   // the main (/v1) chain needs the bean; the /internal chain does not use it

    @Autowired
    private WebApplicationContext wac;

    @Autowired
    private JdbcTemplate jdbc;

    private MockMvc mvc;
    private UUID ruleSetId;
    private UUID tenantId;

    @BeforeEach
    void seedAndBuild() {
        mvc = webAppContextSetup(wac).apply(springSecurity()).build();
        tenantId = UUID.randomUUID();
        ruleSetId = UUID.randomUUID();
        jdbc.update("INSERT INTO tenants (tenant_id, name, rate_limit_per_min) VALUES (?, ?, ?)",
                tenantId, "owner", 600);
        jdbc.update("INSERT INTO rule_sets (rule_set_id, version, tenant_id, name, definition) "
                + "VALUES (?, ?, ?, ?, CAST(? AS jsonb))", ruleSetId, 1, tenantId, "rs", "{\"steps\": []}");
    }

    @Test
    void execServiceCert_reachesInternalRuleSet() throws Exception {
        mvc.perform(get("/internal/rule-sets/{id}/versions/{v}", ruleSetId, 1)
                        .queryParam("tenantId", tenantId.toString())
                        .with(x509(cert("certs/exec-client.pem"))))
                .andExpect(status().isOk())
                .andExpect(content().string(org.hamcrest.Matchers.containsString("steps")));
    }

    @Test
    void gatewayCert_isForbiddenFromInternalRuleSet() throws Exception {
        // A valid internal-CA peer, but NOT the exec subject — this is the cross-tenant read the audit found.
        mvc.perform(get("/internal/rule-sets/{id}/versions/{v}", ruleSetId, 1)
                        .queryParam("tenantId", tenantId.toString())
                        .with(x509(cert("certs/gateway-client.pem"))))
                .andExpect(status().isForbidden());
    }

    @Test
    void noClientCert_isForbidden() throws Exception {
        mvc.perform(get("/internal/rule-sets/{id}/versions/{v}", ruleSetId, 1)
                        .queryParam("tenantId", tenantId.toString()))
                .andExpect(status().isForbidden());
    }

    @Test
    void gatewayCert_reachesTenantPolicy() throws Exception {
        // The gateway's rate-limit policy loader (arch-audit H1): peer-authenticated, minimal projection.
        mvc.perform(get("/internal/tenants/{id}/policy", tenantId)
                        .with(x509(cert("certs/gateway-client.pem"))))
                .andExpect(status().isOk())
                .andExpect(content().string(org.hamcrest.Matchers.containsString("\"rateLimitPerMin\":600")));
    }

    @Test
    void execCert_isForbiddenFromTenantPolicy() throws Exception {
        // Per-path scoping cuts both ways: the exec subject has no business reading tenant policies.
        mvc.perform(get("/internal/tenants/{id}/policy", tenantId)
                        .with(x509(cert("certs/exec-client.pem"))))
                .andExpect(status().isForbidden());
    }

    @Test
    void noClientCert_isForbiddenFromTenantPolicy() throws Exception {
        mvc.perform(get("/internal/tenants/{id}/policy", tenantId))
                .andExpect(status().isForbidden());
    }

    private static X509Certificate cert(String classpathResource) throws Exception {
        try (InputStream in = InternalMtlsAuthorizationIT.class.getClassLoader()
                .getResourceAsStream(classpathResource)) {
            return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(in);
        }
    }
}
