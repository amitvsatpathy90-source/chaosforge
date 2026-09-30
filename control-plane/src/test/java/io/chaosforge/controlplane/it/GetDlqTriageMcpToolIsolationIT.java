package io.chaosforge.controlplane.it;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

import io.chaosforge.controlplane.ai.DlqTriageResult;
import io.chaosforge.controlplane.ai.OllamaTriageClient;

import java.time.Instant;
import java.util.List;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.context.WebApplicationContext;

/** MCP transport version of DlqTriageIT's authorization matrix — OPERATOR-only, cross-tenant. */
class GetDlqTriageMcpToolIsolationIT extends AbstractCpIntegrationTest {

    private static final String DLQ_TOPIC = "mcp-triage-test.DLQ";
    private static final String OPERATOR_MISSING_DLQ_SCOPE_TOKEN = "operator.no-dlq-scope.mcp.token";
    private static final String OPERATOR_TOKEN = "operator.mcp.token";
    private static final String TENANT_READ_TOKEN = "tenant.read.mcp.token";

    @MockitoBean(name = "mcpJwtDecoder")
    private JwtDecoder mcpJwtDecoder;

    @MockitoBean
    private OllamaTriageClient triageClient;   // the LLM seam — everything else real, same as DlqTriageIT

    @Autowired
    private KafkaTemplate<String, byte[]> outboxKafkaTemplate;

    @Autowired
    private WebApplicationContext wac;

    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = webAppContextSetup(wac).apply(springSecurity()).build();
        // OPERATOR role AND chaosforge.dlq scope — the only token that should pass now.
        when(mcpJwtDecoder.decode(OPERATOR_TOKEN))
                .thenReturn(jwtWithRolesAndScope(List.of("OPERATOR"), "chaosforge.dlq"));
        // OPERATOR role but no chaosforge.dlq scope — proves the compose isn't a no-op.
        when(mcpJwtDecoder.decode(OPERATOR_MISSING_DLQ_SCOPE_TOKEN))
                .thenReturn(jwtWithRolesAndScope(List.of("OPERATOR"), "chaosforge.read"));
        // Ordinary tenant client — has a read scope, no OPERATOR role, no dlq scope.
        when(mcpJwtDecoder.decode(TENANT_READ_TOKEN))
                .thenReturn(jwtWithRolesAndScope(List.of("USER"), "chaosforge.read"));
    }

    @Test
    void operatorRoleWithDlqScope_returnsTriageVerdict() throws Exception {
        long offset = produceDlqRecord("STEP_FAILED", "target returned 500");
        when(triageClient.advise(org.mockito.ArgumentMatchers.any())).thenReturn(
                new DlqTriageResult(
                        "target-side failure, not infra", DlqTriageResult.SuggestedAction.INVESTIGATE));

        mvc.perform(mcpToolCall(DLQ_TOPIC, 0, offset, OPERATOR_TOKEN))
                .andExpect(status().isOk())
                .andExpect(content().string(Matchers.containsString("INVESTIGATE")));
    }

    @Test
    void operatorRoleWithoutDlqScope_isDenied_provesComposeIsEnforced() throws Exception {
        long offset = produceDlqRecord("STEP_FAILED", "target returned 500");
        when(triageClient.advise(org.mockito.ArgumentMatchers.any())).thenReturn(
                new DlqTriageResult("should never surface", DlqTriageResult.SuggestedAction.DISCARD));

        String body = mvc.perform(mcpToolCall(DLQ_TOPIC, 0, offset, OPERATOR_MISSING_DLQ_SCOPE_TOKEN))
                .andReturn().getResponse().getContentAsString();

        org.assertj.core.api.Assertions.assertThat(body)
                .as("OPERATOR role alone must not be enough — chaosforge.dlq scope is required too")
                .doesNotContain("should never surface")
                .doesNotContain("DISCARD");
    }

    @Test
    void tenantReadScopeWithoutOperatorRole_isDenied_verdictNeverLeaked() throws Exception {
        long offset = produceDlqRecord("STEP_FAILED", "target returned 500");
        when(triageClient.advise(org.mockito.ArgumentMatchers.any())).thenReturn(
                new DlqTriageResult("should never surface", DlqTriageResult.SuggestedAction.DISCARD));

        String body = mvc.perform(mcpToolCall(DLQ_TOPIC, 0, offset, TENANT_READ_TOKEN))
                .andReturn().getResponse().getContentAsString();

        org.assertj.core.api.Assertions.assertThat(body)
                .as("a plain tenant-read token must never see DLQ triage content — OPERATOR role required")
                .doesNotContain("should never surface")
                .doesNotContain("DISCARD");
    }

    private long produceDlqRecord(String reason, String exceptionMessage) throws Exception {
        ProducerRecord<String, byte[]> record =
                new ProducerRecord<>(DLQ_TOPIC, 0, "key", "avro-payload-bytes".getBytes(UTF_8));
        record.headers()
                .add(new RecordHeader("x-dlq-reason", reason.getBytes(UTF_8)))
                .add(new RecordHeader("x-dlq-attempt", "1".getBytes(UTF_8)))
                .add(new RecordHeader("kafka_dlt-exception-message", exceptionMessage.getBytes(UTF_8)))
                .add(new RecordHeader("kafka_dlt-original-topic", "chaosforge.scenario.commands.v1".getBytes(UTF_8)));
        return outboxKafkaTemplate.send(record).get().getRecordMetadata().offset();
    }

    private static Jwt jwtWithRolesAndScope(List<String> roles, String scope) {
        return Jwt.withTokenValue("t")
                .header("alg", "RS256")
                .issuedAt(Instant.now())
                .expiresAt(Instant.now().plusSeconds(300))
                .claim("tenant_id", java.util.UUID.randomUUID().toString())
                .claim("roles", roles)
                .claim("scope", scope)
                .build();
    }

    private static org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder mcpToolCall(
            String topic, int partition, long offset, String bearerToken) {
        String jsonRpcBody = """
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "get_dlq_triage",
                "arguments": {
                    "topic": "%s",
                    "partition": %d,
                    "offset": %d
                }
            }
        }
        """.formatted(topic, partition, offset);

        return post("/mcp")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + bearerToken)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM)
                .content(jsonRpcBody);
    }
}
