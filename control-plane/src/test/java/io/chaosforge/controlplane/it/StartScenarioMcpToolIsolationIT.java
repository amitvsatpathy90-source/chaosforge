package io.chaosforge.controlplane.it;

import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;
import static org.mockito.Mockito.when;

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

/** Real orchestrator (pure DB, no LLM) exercised through /mcp. */
class StartScenarioMcpToolIsolationIT extends AbstractCpIntegrationTest {

    private static final String OPERATE_TOKEN = "operate.mcp.token";
    private static final String READ_ONLY_TOKEN = "read-only.mcp.token";
    private static final String OTHER_TENANT_OPERATE_TOKEN = "other-tenant.operate.mcp.token";

    @MockitoBean(name = "mcpJwtDecoder")
    private JwtDecoder mcpJwtDecoder;

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
        mvc = webAppContextSetup(wac).apply(springSecurity()).build();

        owner = UUID.randomUUID();
        other = UUID.randomUUID();
        scenarioId = UUID.randomUUID();
        UUID ruleSet = UUID.randomUUID();

        jdbc.update("INSERT INTO tenants (tenant_id, name, rate_limit_per_min) "
                + "VALUES (?, ?, ?)", owner, "owner", 600);
        jdbc.update("INSERT INTO tenants (tenant_id, name, rate_limit_per_min) "
                + "VALUES (?, ?, ?)", other, "other", 600);
        jdbc.update("INSERT INTO rule_sets (rule_set_id, version, tenant_id, name, definition) "
                + "VALUES (?, ?, ?, ?, CAST(? AS jsonb))", ruleSet, 1, owner, "rs", "{}");
        jdbc.update("INSERT INTO scenarios (scenario_id, tenant_id, name, rule_set_id, rule_set_version) "
                + "VALUES (?, ?, ?, ?, ?)", scenarioId, owner, "sc", ruleSet, 1);
        // trigger seeds scenario_replay_state at replay_version = 0

        when(mcpJwtDecoder.decode(OPERATE_TOKEN)).thenReturn(jwtFor(owner, "chaosforge.operate"));
        when(mcpJwtDecoder.decode(READ_ONLY_TOKEN)).thenReturn(jwtFor(owner, "chaosforge.read"));
        when(mcpJwtDecoder.decode(OTHER_TENANT_OPERATE_TOKEN)).thenReturn(jwtFor(other, "chaosforge.operate"));
    }

    @Test
    void freshClaim_returns202EquivalentWithFencingTokenOne() throws Exception {
        mvc.perform(mcpToolCall(scenarioId, 0L, UUID.randomUUID(), OPERATE_TOKEN))
                .andExpect(status().isOk())
                .andExpect(content().string(Matchers.containsString("\"fencingToken\":1")))
                .andExpect(content().string(Matchers.containsString("\"idempotentReplay\":false")));
    }

    @Test
    void sameIdempotencyKeyRetried_returnsOriginalToken_idempotentReplayTrue() throws Exception {
        UUID idemKey = UUID.randomUUID();

        mvc.perform(mcpToolCall(scenarioId, 0L, idemKey, OPERATE_TOKEN)).andExpect(status().isOk());
        mvc.perform(mcpToolCall(scenarioId, 0L, idemKey, OPERATE_TOKEN))
                .andExpect(status().isOk())
                .andExpect(content().string(Matchers.containsString("\"fencingToken\":1")))
                .andExpect(content().string(Matchers.containsString("\"idempotentReplay\":true")));
    }

    @Test
    void staleExpectedVersion_afterAnAdvance_isConflict() throws Exception {
        mvc.perform(mcpToolCall(scenarioId, 0L, UUID.randomUUID(), OPERATE_TOKEN))
                .andExpect(status().isOk());

        // Second attempt still claims expectedVersion=0, but the version already advanced to 1.
        mvc.perform(mcpToolCall(scenarioId, 0L, UUID.randomUUID(), OPERATE_TOKEN))
                .andExpect(status().isOk())   // MCP errors surface as 200 + isError, not an HTTP status
                .andExpect(content().string(Matchers.containsString("concurrent replay")));
    }

    @Test
    void crossTenantScenarioId_and_nonexistentScenarioId_produceIdenticalResponse() throws Exception {
        String crossTenant = mvc.perform(
                mcpToolCall(scenarioId, 0L, UUID.randomUUID(), OTHER_TENANT_OPERATE_TOKEN))
                .andReturn().getResponse().getContentAsString();
        String nonexistent = mvc.perform(
                mcpToolCall(UUID.randomUUID(), 0L, UUID.randomUUID(), OTHER_TENANT_OPERATE_TOKEN))
                .andReturn().getResponse().getContentAsString();

        org.assertj.core.api.Assertions.assertThat(normalize(crossTenant))
                .as("cross-tenant scenarioId must be indistinguishable from a nonexistent one")
                .isEqualTo(normalize(nonexistent));
    }

    @Test
    void readOnlyScopeAlone_isDenied_noOutboxRowInserted() throws Exception {
        Integer before = jdbc.queryForObject(
                "SELECT count(*) FROM outbox WHERE aggregate_id = ?", Integer.class, scenarioId);

        String body = mvc.perform(mcpToolCall(scenarioId, 0L, UUID.randomUUID(), READ_ONLY_TOKEN))
                .andReturn().getResponse().getContentAsString();

        Integer after = jdbc.queryForObject(
                "SELECT count(*) FROM outbox WHERE aggregate_id = ?", Integer.class, scenarioId);
        org.assertj.core.api.Assertions.assertThat(after).isEqualTo(before);
        org.assertj.core.api.Assertions.assertThat(body).doesNotContain("fencingToken");
    }

    private static MockHttpServletRequestBuilder mcpToolCall(UUID scenarioId, long expectedVersion,
                                                             UUID idemKey, String bearerToken) {
        String jsonRpcBody = """
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "start_scenario",
                "arguments": {
                    "scenarioId": "%s",
                    "expectedVersion": %d,
                    "idempotencyKey": "%s"
                }
            }
        }
        """.formatted(scenarioId, expectedVersion, idemKey);

        return post("/mcp")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + bearerToken)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM)
                .content(jsonRpcBody);
    }

    private static String normalize(String jsonRpcResponseBody) {
        return jsonRpcResponseBody
                .replaceAll("\"id\"\\s*:\\s*\\d+", "\"id\":0")
                .replaceAll("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
                        "<uuid>");
    }

    private static Jwt jwtFor(UUID tenantId, String scope) {
        Instant now = Instant.now();
        return Jwt.withTokenValue("t")
                .header("alg", "RS256")
                .claim("tenant_id", tenantId.toString())
                .claim("roles", List.of("USER"))
                .claim("scope", scope)
                .issuedAt(now)
                .expiresAt(now.plusSeconds(300))
                .build();
    }
}
