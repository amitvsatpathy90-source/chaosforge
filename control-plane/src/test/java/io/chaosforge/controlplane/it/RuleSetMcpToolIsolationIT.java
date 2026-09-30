package io.chaosforge.controlplane.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

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
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.web.context.WebApplicationContext;

/** Tenant isolation and scope enforcement for get_rule_set / list_rule_sets over the real /mcp chain. */
class RuleSetMcpToolIsolationIT extends AbstractCpIntegrationTest {

    private static final String OWNER_READ = "owner.read.token";
    private static final String OTHER_READ = "other.read.token";
    private static final String OWNER_WRONG_SCOPE = "owner.operate-only.token";
    private static final String NAME_MARKER = "owner-only-rule-set-marker";

    @MockitoBean(name = "mcpJwtDecoder")
    private JwtDecoder mcpJwtDecoder;

    @Autowired
    private WebApplicationContext wac;

    @Autowired
    private JdbcTemplate jdbc;

    private MockMvc mvc;
    private UUID ruleSetId;

    @BeforeEach
    void seedAndStubTokens() {
        mvc = webAppContextSetup(wac).apply(springSecurity()).build();
        UUID owner = UUID.randomUUID();
        UUID other = UUID.randomUUID();
        ruleSetId = UUID.randomUUID();

        jdbc.update("INSERT INTO tenants (tenant_id, name, rate_limit_per_min) VALUES (?, ?, ?)", owner, "owner", 600);
        jdbc.update("INSERT INTO tenants (tenant_id, name, rate_limit_per_min) VALUES (?, ?, ?)", other, "other", 600);
        jdbc.update("INSERT INTO rule_sets (rule_set_id, version, tenant_id, name, definition) "
                + "VALUES (?, ?, ?, ?, CAST(? AS jsonb))", ruleSetId, 1, owner, NAME_MARKER, "{}");

        when(mcpJwtDecoder.decode(OWNER_READ)).thenReturn(jwtFor(owner, "chaosforge.read"));
        when(mcpJwtDecoder.decode(OTHER_READ)).thenReturn(jwtFor(other, "chaosforge.read"));
        when(mcpJwtDecoder.decode(OWNER_WRONG_SCOPE)).thenReturn(jwtFor(owner, "chaosforge.operate"));
    }

    // ADR-0510 parity: full-body equality, only the JSON-RPC id and UUIDs normalized.
    @Test
    void crossTenantRuleSet_and_nonexistentRuleSet_produceIdenticalResponse() throws Exception {
        String cross = body(call("get_rule_set", getArgs(ruleSetId), OTHER_READ));
        String missing = body(call("get_rule_set", getArgs(UUID.randomUUID()), OTHER_READ));

        assertThat(normalize(cross)).isEqualTo(normalize(missing));
    }

    @Test
    void ownerToken_withReadScope_getReturnsOwnRuleSet() throws Exception {
        call("get_rule_set", getArgs(ruleSetId), OWNER_READ)
                .andExpect(status().isOk())
                .andExpect(content().string(containsString(NAME_MARKER)));
    }

    @Test
    void ownerToken_withoutReadScope_getIsDeniedNotLeaked() throws Exception {
        assertThat(body(call("get_rule_set", getArgs(ruleSetId), OWNER_WRONG_SCOPE)))
                .doesNotContain(NAME_MARKER);
    }

    @Test
    void ownerToken_withReadScope_listReturnsOwnRuleSet() throws Exception {
        call("list_rule_sets", "{}", OWNER_READ)
                .andExpect(status().isOk())
                .andExpect(content().string(containsString(ruleSetId.toString())));
    }

    @Test
    void otherTenantToken_listNeverContainsOwnersRuleSet() throws Exception {
        assertThat(body(call("list_rule_sets", "{}", OTHER_READ)))
                .doesNotContain(ruleSetId.toString())
                .doesNotContain(NAME_MARKER);
    }

    @Test
    void ownerToken_withoutReadScope_listIsDeniedNotLeaked() throws Exception {
        assertThat(body(call("list_rule_sets", "{}", OWNER_WRONG_SCOPE)))
                .doesNotContain(NAME_MARKER);
    }

    private static String getArgs(UUID id) {
        return "{\"ruleSetId\":\"" + id + "\",\"version\":1}";
    }

    private ResultActions call(String tool, String argumentsJson, String token) throws Exception {
        String rpc = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\","
                + "\"params\":{\"name\":\"" + tool + "\",\"arguments\":" + argumentsJson + "}}";
        return mvc.perform(post("/mcp")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM)   // stateless needs both
                .content(rpc));
    }

    private static String body(ResultActions r) throws Exception {
        return r.andReturn().getResponse().getContentAsString();
    }

    private static String normalize(String json) {
        return json.replaceAll("\"id\"\\s*:\\s*\\d+", "\"id\":0")
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
