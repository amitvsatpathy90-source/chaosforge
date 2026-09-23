package io.chaosforge.controlplane.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import io.chaosforge.controlplane.mcp.ScenarioMcpTools;
import io.chaosforge.controlplane.service.ScenarioService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.web.context.WebApplicationContext;

/**
 * Proves tenant isolation and scope authorization on the MCP invocation path — through the
 * {@code /mcp} transport into {@link ScenarioMcpTools#getScenario}, not only inside
 * {@link ScenarioService}.
 *
 * <p>The integration cases verify MCP-specific security properties not established by the existing
 * REST tests: ADR-0510 indistinguishability for cross-tenant versus nonexistent scenarios, and
 * enforcement of the tool-level {@code chaosforge.read} scope.
 *
 * <p>The test uses the real Spring Security filter chain and real Postgres, with JWT decoding
 * replaced by a deterministic test double.
 */
class ScenarioMcpToolIsolationIT extends AbstractCpIntegrationTest {

    // Opaque values; only the stubbed mcpJwtDecoder.decode() result matters to the filter chain.
    private static final String OTHER_TENANT_READ_TOKEN = "other.read.token";
    private static final String OWNER_READ_TOKEN = "owner.read.token";
    // Valid tenant, wrong scope — proves scope is checked, not just audience.
    private static final String OWNER_WRONG_SCOPE_TOKEN = "owner.operate-only.token";

    @MockitoBean(name = "mcpJwtDecoder")
    private JwtDecoder mcpJwtDecoder;

    @Autowired ApplicationContext ctx;

    @Autowired
    private WebApplicationContext wac;

    @Autowired
    private JdbcTemplate jdbc;

    private MockMvc mvc;
    private UUID owner;
    private UUID other;
    private UUID ownerScenarioId;

    @BeforeEach
    void seedAndStubTokens() {
        // Real filter chain — proves @PreAuthorize + MCP engine interaction, not just the tool method.
        mvc = webAppContextSetup(wac).apply(springSecurity()).build();

        owner = UUID.randomUUID();
        other = UUID.randomUUID();
        ownerScenarioId = UUID.randomUUID();
        UUID ruleSet = UUID.randomUUID();

        // Fresh UUIDs per test — no cross-test cache bleed on the tenant-scoped scenario cache key
        // (ScenarioService keys its cache as "<tenantId>:<scenarioId>").
        jdbc.update("INSERT INTO tenants (tenant_id, name, rate_limit_per_min) VALUES (?, ?, ?)",
                owner, "owner", 600);
        jdbc.update("INSERT INTO tenants (tenant_id, name, rate_limit_per_min) VALUES (?, ?, ?)",
                other, "other", 600);
        jdbc.update("INSERT INTO rule_sets (rule_set_id, version, tenant_id, name, definition) "
                + "VALUES (?, ?, ?, ?, CAST(? AS jsonb))", ruleSet, 1, owner, "rs", "{}");
        jdbc.update("INSERT INTO scenarios (scenario_id, tenant_id, name, rule_set_id, rule_set_version) "
                + "VALUES (?, ?, ?, ?, ?)", ownerScenarioId, owner, "sc", ruleSet, 1);

        when(mcpJwtDecoder.decode(OTHER_TENANT_READ_TOKEN))
                .thenReturn(jwtFor(other, "chaosforge.read"));
        when(mcpJwtDecoder.decode(OWNER_READ_TOKEN))
                .thenReturn(jwtFor(owner, "chaosforge.read"));
        when(mcpJwtDecoder.decode(OWNER_WRONG_SCOPE_TOKEN))
                // Valid tenant, valid token — just missing the SCOPE_chaosforge.read authority the
                // tool method's @PreAuthorize requires. Proves scope is checked, not just audience.
                .thenReturn(jwtFor(owner, "chaosforge.operate"));
    }

    /**
     * Cross-tenant vs nonexistent scenarioId must be indistinguishable (ADR-0510 parity for MCP).
     * Asserts full response equality, not a specific status — shape-agnostic to the JSON-RPC error envelope.
     */
    @Test
    void crossTenantScenarioId_and_nonexistentScenarioId_produceIdenticalResponse() throws Exception {
        String crossTenantBody = mvc.perform(mcpToolCall(ownerScenarioId, OTHER_TENANT_READ_TOKEN))
                .andReturn().getResponse().getContentAsString();

        String nonexistentBody = mvc.perform(mcpToolCall(UUID.randomUUID(), OTHER_TENANT_READ_TOKEN))
                .andReturn().getResponse().getContentAsString();

        // Normalize the JSON-RPC request "id" echo only — every other byte (error code, message, any
        // data field) must match exactly.
        assertThat(normalizeRequestId(crossTenantBody))
                .as("a cross-tenant scenarioId must be indistinguishable from a nonexistent one")
                .isEqualTo(normalizeRequestId(nonexistentBody));
    }

    /** Positive control — proves the chain wires up before trusting the isolation test above. */
    @Test
    void ownerToken_withReadScope_returnsOwnScenario() throws Exception {
        mvc.perform(mcpToolCall(ownerScenarioId, OWNER_READ_TOKEN))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString(ownerScenarioId.toString())))
                .andExpect(content().string(containsString(owner.toString())));
    }

    /**
     * Denial must not leak the scenario. Status (403 vs in-band JSON-RPC error) is left unpinned —
     * this test observes which the MCP engine actually does; record it for Phase 3.
     */
    @Test
    void ownerToken_withoutReadScope_isDeniedNotLeaked() throws Exception {
        String body = mvc.perform(mcpToolCall(ownerScenarioId, OWNER_WRONG_SCOPE_TOKEN))
                .andDo(print())
                .andReturn().getResponse().getContentAsString();

        assertThat(body)
                .as("a scope-denied call must never return the scenario it was denied access to")
                .doesNotContain(ownerScenarioId.toString());
    }

    @Test
    void diagnostic_mcpRouterFunctionBeanExists() {
        System.out.println("RouterFunction beans: "
                + java.util.Arrays.toString(ctx.getBeanNamesForType(
                org.springframework.web.servlet.function.RouterFunction.class)));
        System.out.println("McpStatelessServerTransport beans: "
                + java.util.Arrays.toString(ctx.getBeanNamesForType(
                Class.forName("org.springframework.ai.mcp.server.stateless.WebMvcStatelessServerTransport"))));
    }

    /** JSON-RPC tools/call POST for get_scenario. */
    private static MockHttpServletRequestBuilder mcpToolCall(UUID scenarioId, String bearerToken) {
        String jsonRpcBody = """
                {"jsonrpc":"2.0","id":1,"method":"tools/call",\
                "params":{"name":"get_scenario","arguments":{"scenarioId":"%s"}}}\
                """.formatted(scenarioId);

        return post("/mcp")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + bearerToken)
                .contentType(MediaType.APPLICATION_JSON)
                // STATELESS protocol: no SSE stream — a plain JSON response is expected, not
                // text/event-stream (see application.yml chaosforge.ai.mcp.server.protocol).
                .accept(MediaType.APPLICATION_JSON)
                .content(jsonRpcBody);
    }

    /** Strips the JSON-RPC id echo so a non-constant id later doesn't break the equality check. */
    private static String normalizeRequestId(String jsonRpcResponseBody) {
        return jsonRpcResponseBody.replaceAll("\"id\"\\s*:\\s*\\d+", "\"id\":0");
    }

    private static Jwt jwtFor(UUID tenantId, String scope) {
        Instant now = Instant.now();
        return Jwt.withTokenValue("t")
                .header("alg", "RS256")
                .subject("user@" + tenantId)
                .claim("tenant_id", tenantId.toString())
                .claim("roles", List.of("USER"))
                .claim("scope", scope)
                .issuedAt(now)
                .expiresAt(now.plusSeconds(300))
                .build();
    }
}
