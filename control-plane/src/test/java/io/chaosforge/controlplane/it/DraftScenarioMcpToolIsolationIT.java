package io.chaosforge.controlplane.it;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

import io.chaosforge.controlplane.ai.OllamaAuthoringClient;
import io.chaosforge.controlplane.ai.ScenarioDraft;
import io.chaosforge.controlplane.ai.TenantTargetValidator;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.context.WebApplicationContext;

/**
 * Auth matrix for draft_scenario: chaosforge.operate required, chaosforge.read alone denied.
 * TenantTargetValidator is mocked to a no-op — this IT is scoped to auth, not SSRF policy (which
 * does a real DNS lookup and belongs to target-validation-rules.md's own test suite, not here).
 */
class DraftScenarioMcpToolIsolationIT extends AbstractCpIntegrationTest {

    private static final String OPERATE_SCOPE_TOKEN = "operate.mcp.token";
    private static final String READ_ONLY_SCOPE_TOKEN = "read-only.mcp.token";

    @MockitoBean(name = "mcpJwtDecoder")
    private JwtDecoder mcpJwtDecoder;

    @MockitoBean
    private OllamaAuthoringClient authoringClient;

    @MockitoBean
    private TenantTargetValidator tenantTargetValidator;

    @Autowired
    private WebApplicationContext wac;

    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = webAppContextSetup(wac).apply(springSecurity()).build();
        doNothing().when(tenantTargetValidator).assertOwned(any(), any());
        when(mcpJwtDecoder.decode(OPERATE_SCOPE_TOKEN)).thenReturn(jwtWithScope("chaosforge.operate"));
        when(mcpJwtDecoder.decode(READ_ONLY_SCOPE_TOKEN)).thenReturn(jwtWithScope("chaosforge.read"));
    }

    @Test
    void operateScope_returnsGeneratedDraft() throws Exception {
        when(authoringClient.generate(any())).thenReturn(new ScenarioDraft("kill-payments", "summary",
                List.of(new ScenarioDraft.StepDraft(
                        "s1", "https://example.com/health", "GET", 0))));

        mvc.perform(mcpToolCall("kill the payments pod for 30s", OPERATE_SCOPE_TOKEN))
                .andExpect(status().isOk())
                .andExpect(content().string(Matchers.containsString("kill-payments")));
    }

    @Test
    void readOnlyScopeAlone_isDenied_draftNeverGenerated() throws Exception {
        when(authoringClient.generate(any())).thenReturn(new ScenarioDraft("should-not-appear", "summary",
                List.of(new ScenarioDraft.StepDraft("s1", "https://example.com/health", "GET", 0))));

        String body = mvc.perform(mcpToolCall("kill the payments pod for 30s", READ_ONLY_SCOPE_TOKEN))
                .andReturn().getResponse().getContentAsString();

        org.assertj.core.api.Assertions.assertThat(body)
                .as("chaosforge.read alone must not authorize draft generation — operate scope required")
                .doesNotContain("should-not-appear");
    }

    private static org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder mcpToolCall(
            String description, String bearerToken) {
        String jsonRpcBody = """
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "draft_scenario",
                "arguments": {
                    "description": "%s"
                }
            }
        }
        """.formatted(description);

        return post("/mcp")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + bearerToken)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM)
                .content(jsonRpcBody);
    }

    private static Jwt jwtWithScope(String scope) {
        return Jwt.withTokenValue("t")
                .header("alg", "RS256")
                .issuedAt(Instant.now())
                .expiresAt(Instant.now().plusSeconds(300))
                .claim("tenant_id", UUID.randomUUID().toString())
                .claim("roles", List.of("USER"))
                .claim("scope", scope)
                .build();
    }
}
