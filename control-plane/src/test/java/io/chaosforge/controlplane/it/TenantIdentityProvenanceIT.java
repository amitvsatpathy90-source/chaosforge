package io.chaosforge.controlplane.it;

import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.oauth2.jwt.BadJwtException;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.context.WebApplicationContext;

/**
 * Acceptance gate <b>C10</b> — tenant identity is derived from the <em>verified</em> JWT, never from
 * the {@code X-Tenant-Id} header (ADR-0524). This is the HTTP-layer proof the deferred matrix called
 * out: the orchestrator-level tests cover the tenant-scoped CAS, but only a request driven through the
 * real Spring Security filter chain proves the <i>provenance</i> of {@code tenant_id}.
 *
 * <p>The single load-bearing case is {@link #crossTenantJwt_spoofingTheOwnerHeader_is404_noLeak()}:
 * a caller holding tenant B's JWT spoofs {@code X-Tenant-Id: <tenant A>} on tenant A's scenario. If
 * the header were authoritative — the exact bug ADR-0524 exists to prevent — this would return tenant
 * A's data. It returns <b>404</b> (never 403, no {@code ETag}/version disclosed): the
 * {@link io.chaosforge.controlplane.security.JwtTenantExtractionFilter} binds the tenant from the
 * verified claim (B), and B's tenant-scoped lookup of A's scenario is empty (ADR-0510).
 *
 * <p>The {@link JwtDecoder} is mocked so a test token decodes to a chosen {@code tenant_id} claim
 * without standing up a JWKS endpoint — the filter, the filter chain, and the three-layer isolation
 * all run for real against Postgres.
 */
class TenantIdentityProvenanceIT extends AbstractCpIntegrationTest {

    private static final String OWNER_TOKEN = "owner.jwt.token";
    private static final String OTHER_TOKEN = "other.jwt.token";
    private static final String GARBAGE_TOKEN = "not.a.valid.token";
    private static final String NO_TENANT_TOKEN = "no.tenant.token";

    @MockitoBean
    private JwtDecoder jwtDecoder;

    @Autowired
    private WebApplicationContext wac;

    @Autowired
    private JdbcTemplate jdbc;

    private MockMvc mvc;
    private UUID owner;
    private UUID other;
    private UUID scenarioId;

    @BeforeEach
    void seedAndStubTokens() {
        // Build MockMvc with the real Spring Security filter chain applied (Boot 4 moved
        // @AutoConfigureMockMvc to a separate module; webAppContextSetup needs only spring-test).
        mvc = webAppContextSetup(wac).apply(springSecurity()).build();

        owner = UUID.randomUUID();
        other = UUID.randomUUID();
        scenarioId = UUID.randomUUID();
        UUID ruleSet = UUID.randomUUID();

        // Fresh UUIDs per test → no cross-test cache bleed on the tenant-scoped scenario cache key.
        jdbc.update("INSERT INTO tenants (tenant_id, name, rate_limit_per_min) VALUES (?, ?, ?)",
                owner, "owner", 600);
        jdbc.update("INSERT INTO tenants (tenant_id, name, rate_limit_per_min) VALUES (?, ?, ?)",
                other, "other", 600);
        jdbc.update("INSERT INTO rule_sets (rule_set_id, version, tenant_id, name, definition) "
                + "VALUES (?, ?, ?, ?, CAST(? AS jsonb))", ruleSet, 1, owner, "rs", "{}");
        jdbc.update("INSERT INTO scenarios (scenario_id, tenant_id, name, rule_set_id, rule_set_version) "
                + "VALUES (?, ?, ?, ?, ?)", scenarioId, owner, "sc", ruleSet, 1);

        when(jwtDecoder.decode(OWNER_TOKEN)).thenReturn(jwtFor(owner));
        when(jwtDecoder.decode(OTHER_TOKEN)).thenReturn(jwtFor(other));
        when(jwtDecoder.decode(GARBAGE_TOKEN)).thenThrow(new BadJwtException("signature mismatch"));
        when(jwtDecoder.decode(NO_TENANT_TOKEN)).thenReturn(jwtWithoutTenant());
    }

    @Test
    void ownerJwt_seesItsOwnScenario() throws Exception {
        mvc.perform(get("/v1/scenarios/{id}", scenarioId).header(HttpHeaders.AUTHORIZATION, bearer(OWNER_TOKEN)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.scenarioId").value(scenarioId.toString()))
                .andExpect(jsonPath("$.tenantId").value(owner.toString()));
    }

    /**
     * THE C10 proof. Tenant B's JWT + a spoofed {@code X-Tenant-Id} naming the owning tenant A. A
     * header-trusting implementation would leak A's scenario; the verified JWT (B) wins → 404, and no
     * {@code ETag} (replay version) is disclosed.
     */
    @Test
    void crossTenantJwt_spoofingTheOwnerHeader_is404_noLeak() throws Exception {
        mvc.perform(get("/v1/scenarios/{id}", scenarioId)
                        .header(HttpHeaders.AUTHORIZATION, bearer(OTHER_TOKEN))
                        .header("X-Tenant-Id", owner.toString()))   // spoof: claim to be the owner
                .andExpect(status().isNotFound())
                .andExpect(header().doesNotExist(HttpHeaders.ETAG));
    }

    /**
     * The inverse: the owner's JWT with a spoofed {@code X-Tenant-Id} naming a <i>different</i> tenant
     * still returns the owner's scenario. The header is inert in both directions — only the JWT decides.
     */
    @Test
    void ownerJwt_spoofingAnotherTenantHeader_stillSeesOwnScenario() throws Exception {
        mvc.perform(get("/v1/scenarios/{id}", scenarioId)
                        .header(HttpHeaders.AUTHORIZATION, bearer(OWNER_TOKEN))
                        .header("X-Tenant-Id", other.toString()))   // spoof: claim to be someone else
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tenantId").value(owner.toString()));
    }

    @Test
    void noBearerToken_is401() throws Exception {
        mvc.perform(get("/v1/scenarios/{id}", scenarioId))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void invalidJwt_is401_andTheHeaderCannotRescueIt() throws Exception {
        mvc.perform(get("/v1/scenarios/{id}", scenarioId)
                        .header(HttpHeaders.AUTHORIZATION, bearer(GARBAGE_TOKEN))
                        .header("X-Tenant-Id", owner.toString()))   // header is not an auth fallback
                .andExpect(status().isUnauthorized());
    }

    /**
     * SEC-03: a validly-signed token with NO {@code tenant_id} claim (an IdP service/machine token) is
     * 401, not 500. Pre-fix, {@code UUID.fromString(null)} threw an NPE outside the filter's
     * {@code JwtException | IllegalArgumentException} catch → a 500. The {@code X-Tenant-Id} header must
     * not rescue it either.
     */
    @Test
    void tokenWithoutTenantClaim_is401_not500() throws Exception {
        mvc.perform(get("/v1/scenarios/{id}", scenarioId)
                        .header(HttpHeaders.AUTHORIZATION, bearer(NO_TENANT_TOKEN))
                        .header("X-Tenant-Id", owner.toString()))
                .andExpect(status().isUnauthorized());
    }

    private static Jwt jwtWithoutTenant() {
        Instant now = Instant.now();
        return Jwt.withTokenValue("t")
                .header("alg", "RS256")
                .subject("service-account")
                .claim("roles", List.of("USER"))   // authenticated, but carries no tenant_id
                .issuedAt(now)
                .expiresAt(now.plusSeconds(300))
                .build();
    }

    private static Jwt jwtFor(UUID tenantId) {
        Instant now = Instant.now();
        return Jwt.withTokenValue("t")
                .header("alg", "RS256")
                .subject("user@" + tenantId)
                .claim("tenant_id", tenantId.toString())
                .claim("roles", List.of("USER"))
                .issuedAt(now)
                .expiresAt(now.plusSeconds(300))
                .build();
    }

    private static String bearer(String token) {
        return "Bearer " + token;
    }
}
