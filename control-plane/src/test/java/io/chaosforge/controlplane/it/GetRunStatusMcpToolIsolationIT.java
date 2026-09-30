package io.chaosforge.controlplane.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.web.context.WebApplicationContext;

/** Mirrors ScenarioMcpToolIsolationIT — proves the same MCP-transport guarantees for get_run_status. */
class GetRunStatusMcpToolIsolationIT extends AbstractCpIntegrationTest {

    private static final String OTHER_TENANT_READ_TOKEN = "other.read.token";
    private static final String OWNER_READ_TOKEN = "owner.read.token";
    private static final String OWNER_WRONG_SCOPE_TOKEN = "owner.operate-only.token";

    @MockitoBean(name = "mcpJwtDecoder")
    private JwtDecoder mcpJwtDecoder;

    @Autowired
    private WebApplicationContext wac;

    @Autowired
    private JdbcTemplate jdbc;

    private MockMvc mvc;
    private UUID owner;
    private UUID other;
    private UUID terminalScenarioId;
    private UUID inProgressScenarioId;

    @BeforeEach
    void seedAndStubTokens() {
        mvc = webAppContextSetup(wac).apply(springSecurity()).build();

        owner = UUID.randomUUID();
        other = UUID.randomUUID();
        terminalScenarioId = UUID.randomUUID();
        inProgressScenarioId = UUID.randomUUID();
        UUID ruleSet = UUID.randomUUID();

        jdbc.update("INSERT INTO tenants (tenant_id, name, rate_limit_per_min) VALUES (?, ?, ?)",
                owner, "owner", 600);
        jdbc.update("INSERT INTO tenants (tenant_id, name, rate_limit_per_min) VALUES (?, ?, ?)",
                other, "other", 600);
        jdbc.update("INSERT INTO rule_sets (rule_set_id, version, tenant_id, name, definition) "
                + "VALUES (?, ?, ?, ?, CAST(? AS jsonb))", ruleSet, 1, owner, "rs", "{}");
        jdbc.update("INSERT INTO scenarios (scenario_id, tenant_id, name, rule_set_id, rule_set_version) "
                + "VALUES (?, ?, ?, ?, ?)", terminalScenarioId, owner, "sc-terminal", ruleSet, 1);
        jdbc.update("INSERT INTO scenarios (scenario_id, tenant_id, name, rule_set_id, rule_set_version) "
                + "VALUES (?, ?, ?, ?, ?)", inProgressScenarioId, owner, "sc-in-progress", ruleSet, 1);

        // trigger seeds scenario_replay_state at replay_version=0 for both — terminal gets a
        // matching run_projection row at that version, in-progress deliberately has none.
        jdbc.update("INSERT INTO run_projection (scenario_id, replay_version, tenant_id, outcome, finished_at) "
                + "VALUES (?, 0, ?, 'COMPLETED', now())", terminalScenarioId, owner);

        when(mcpJwtDecoder.decode(OTHER_TENANT_READ_TOKEN)).thenReturn(jwtFor(other, "chaosforge.read"));
        when(mcpJwtDecoder.decode(OWNER_READ_TOKEN)).thenReturn(jwtFor(owner, "chaosforge.read"));
        when(mcpJwtDecoder.decode(OWNER_WRONG_SCOPE_TOKEN)).thenReturn(jwtFor(owner, "chaosforge.operate"));
    }

    @Test
    void crossTenantScenarioId_and_nonexistentScenarioId_produceIdenticalResponse() throws Exception {
        String crossTenantBody = mvc.perform(mcpToolCall(terminalScenarioId, OTHER_TENANT_READ_TOKEN))
                .andReturn().getResponse().getContentAsString();

        String nonexistentBody = mvc.perform(mcpToolCall(UUID.randomUUID(), OTHER_TENANT_READ_TOKEN))
                .andReturn().getResponse().getContentAsString();

        assertThat(normalizeResponse(crossTenantBody))
                .as("a cross-tenant scenarioId must be indistinguishable from a nonexistent one")
                .isEqualTo(normalizeResponse(nonexistentBody));
    }

    @Test
    void ownerToken_terminalScenario_returnsTerminalStatus() throws Exception {
        mvc.perform(mcpToolCall(terminalScenarioId, OWNER_READ_TOKEN))
                .andExpect(status().isOk())
                .andExpect(content().string(Matchers.containsString("TERMINAL")))
                .andExpect(content().string(Matchers.containsString("COMPLETED")));
    }

    @Test
    void ownerToken_scenarioWithNoTerminalEvent_returnsInProgress() throws Exception {
        mvc.perform(mcpToolCall(inProgressScenarioId, OWNER_READ_TOKEN))
                .andExpect(status().isOk())
                .andExpect(content().string(Matchers.containsString("IN_PROGRESS")))
                .andExpect(content().string(Matchers.not(Matchers.containsString("COMPLETED"))));
    }

    @Test
    void ownerToken_withoutReadScope_isDeniedNotLeaked() throws Exception {
        String body = mvc.perform(mcpToolCall(terminalScenarioId, OWNER_WRONG_SCOPE_TOKEN))
                .andReturn().getResponse().getContentAsString();

        assertThat(body)
                .as("a scope-denied call must never return the run status it was denied access to")
                .doesNotContain(terminalScenarioId.toString())
                .doesNotContain("COMPLETED");
    }

    private static MockHttpServletRequestBuilder mcpToolCall(UUID scenarioId, String bearerToken) {
        String jsonRpcBody = """
                {"jsonrpc":"2.0","id":1,"method":"tools/call",\
                "params":{"name":"get_run_status","arguments":{"scenarioId":"%s"}}}\
                """.formatted(scenarioId);

        return post("/mcp")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + bearerToken)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM)
                .content(jsonRpcBody);
    }

    private static String normalizeResponse(String jsonRpcResponseBody) {
        return jsonRpcResponseBody
                .replaceAll("\"id\"\\s*:\\s*\\d+", "\"id\":0")
                .replaceAll("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", "<uuid>");
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
