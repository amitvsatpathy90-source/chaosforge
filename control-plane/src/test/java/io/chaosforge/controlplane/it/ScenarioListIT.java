package io.chaosforge.controlplane.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.context.WebApplicationContext;

/**
 * {@code GET /v1/scenarios} keyset pagination — previously zero coverage (no test in this repo ever
 * exercised the list endpoint before this ADR-scoped pagination change).
 */
class ScenarioListIT extends AbstractCpIntegrationTest {

    private static final String OWNER_TOKEN = "owner.jwt.token";
    private static final String OTHER_TOKEN = "other.jwt.token";

    @MockitoBean
    private JwtDecoder jwtDecoder;

    @Autowired
    private WebApplicationContext wac;

    @Autowired
    private JdbcTemplate jdbc;

    private final ObjectMapper mapper = new ObjectMapper();

    private MockMvc mvc;
    private UUID owner;
    private UUID other;
    private List<UUID> ownerScenarioIdsNewestFirst;

    @BeforeEach
    void seedAndStubTokens() {
        mvc = webAppContextSetup(wac).apply(springSecurity()).build();

        owner = UUID.randomUUID();
        other = UUID.randomUUID();
        UUID ruleSet = UUID.randomUUID();

        jdbc.update("INSERT INTO tenants (tenant_id, name, rate_limit_per_min) VALUES (?, ?, ?)",
                owner, "owner", 600);
        jdbc.update("INSERT INTO tenants (tenant_id, name, rate_limit_per_min) VALUES (?, ?, ?)",
                other, "other", 600);
        jdbc.update("INSERT INTO rule_sets (rule_set_id, version, tenant_id, name, definition) "
                + "VALUES (?, ?, ?, ?, CAST(? AS jsonb))", ruleSet, 1, owner, "rs", "{}");

        // 5 owner scenarios, explicit created_at 1s apart -> deterministic DESC ordering across pages.
        Instant base = Instant.now().minusSeconds(100);
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            UUID id = UUID.randomUUID();
            ids.add(id);
            Instant createdAt = base.plusSeconds(i);
            jdbc.update("INSERT INTO scenarios "
                            + "(scenario_id, tenant_id, name, rule_set_id, rule_set_version, created_at, updated_at) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    id, owner, "sc-" + i, ruleSet, 1,
                    java.sql.Timestamp.from(createdAt), java.sql.Timestamp.from(createdAt));
        }
        // Newest first (DESC) — reverse insertion order.
        ownerScenarioIdsNewestFirst = new ArrayList<>(ids);
        java.util.Collections.reverse(ownerScenarioIdsNewestFirst);

        // One scenario for `other`, under its own rule set — isolation check.
        UUID otherRuleSet = UUID.randomUUID();
        jdbc.update("INSERT INTO rule_sets (rule_set_id, version, tenant_id, name, definition) "
                + "VALUES (?, ?, ?, ?, CAST(? AS jsonb))", otherRuleSet, 1, other, "rs", "{}");
        jdbc.update("INSERT INTO scenarios (scenario_id, tenant_id, name, rule_set_id, rule_set_version) "
                + "VALUES (?, ?, ?, ?, ?)", UUID.randomUUID(), other, "other-sc", otherRuleSet, 1);

        when(jwtDecoder.decode(OWNER_TOKEN)).thenReturn(jwtFor(owner));
        when(jwtDecoder.decode(OTHER_TOKEN)).thenReturn(jwtFor(other));
    }

    @Test
    void list_neverReturnsAnotherTenantsScenarios() throws Exception {
        String body = mvc.perform(get("/v1/scenarios").param("limit", "50")
                        .header(HttpHeaders.AUTHORIZATION, bearer(OTHER_TOKEN)))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        JsonNode items = mapper.readTree(body).get("items");
        assertThat(items).hasSize(1);
        assertThat(items.get(0).get("tenantId").asText()).isEqualTo(other.toString());
    }

    @Test
    void pagination_walksAllPagesInOrder_withNoDuplicatesAndTerminatesWithNullCursor() throws Exception {
        List<UUID> seen = new ArrayList<>();
        String cursor = null;

        // limit=2 over 5 rows -> 3 pages (2, 2, 1), last page's nextCursor must be null.
        for (int page = 0; page < 3; page++) {
            var request = get("/v1/scenarios").param("limit", "2")
                    .header(HttpHeaders.AUTHORIZATION, bearer(OWNER_TOKEN));
            if (cursor != null) {
                request = request.param("cursor", cursor);
            }
            String body = mvc.perform(request).andExpect(status().isOk())
                    .andReturn().getResponse().getContentAsString();

            JsonNode json = mapper.readTree(body);
            json.get("items").forEach(item -> seen.add(UUID.fromString(item.get("scenarioId").asText())));
            cursor = json.get("nextCursor").isNull() ? null : json.get("nextCursor").asText();
        }

        assertThat(cursor).as("final page must not carry a next cursor").isNull();
        assertThat(seen).as("no duplicates/gaps across pages").containsExactlyElementsOf(ownerScenarioIdsNewestFirst);
    }

    @Test
    void badCursor_is400() throws Exception {
        mvc.perform(get("/v1/scenarios").param("cursor", "not-valid-base64!!!")
                        .header(HttpHeaders.AUTHORIZATION, bearer(OWNER_TOKEN)))
                .andExpect(status().isBadRequest());
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
