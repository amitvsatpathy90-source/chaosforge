package io.chaosforge.controlplane.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

import io.chaosforge.controlplane.ai.DlqEnvelope;
import io.chaosforge.controlplane.ai.DlqRecordReader;
import io.chaosforge.controlplane.ai.DlqTriageResult;
import io.chaosforge.controlplane.ai.OllamaTriageClient;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.context.WebApplicationContext;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * DLQ triage tier, end-to-end minus the LLM (the model call is the one mocked seam):
 *
 * <ul>
 *   <li><b>Reader against a real broker</b> (embedded Kafka): point lookup by (topic, partition,
 *       offset); envelope carries ONLY the redacted failure shape — the tenant header and payload
 *       planted on the record must not surface (ADR-0519); first-line-only exception summary;
 *       missing offset → empty; non-DLQ topic → rejected (this endpoint must never become a
 *       generic Kafka reader).</li>
 *   <li><b>Authorization</b> (ADR-0536): OPERATOR role → 200; plain tenant token → 403; no token
 *       → 401. Authentication is not authorization.</li>
 * </ul>
 */
class DlqTriageIT extends AbstractCpIntegrationTest {

    private static final String DLQ_TOPIC = "triage-test.DLQ";
    private static final String OPERATOR_TOKEN = "operator.jwt.token";
    private static final String TENANT_TOKEN = "tenant.jwt.token";

    @MockitoBean
    private JwtDecoder jwtDecoder;

    @MockitoBean
    private OllamaTriageClient triageClient;   // the LLM seam — everything else is real

    @Autowired
    private DlqRecordReader reader;

    @Autowired
    private KafkaTemplate<String, byte[]> outboxKafkaTemplate;

    @Autowired
    private WebApplicationContext wac;

    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = webAppContextSetup(wac).apply(springSecurity()).build();
        when(jwtDecoder.decode(OPERATOR_TOKEN)).thenReturn(jwtWithRoles(List.of("OPERATOR")));
        when(jwtDecoder.decode(TENANT_TOKEN)).thenReturn(jwtWithRoles(List.of("USER")));
    }

    @Test
    void reader_returnsRedactedEnvelope_fromRealBroker() throws Exception {
        long offset = produceDlqRecord("STEP_TIMEOUT",
                "step aggregate deadline exceeded\n\tat io.chaosforge.execution.ExecutePhase.run(...)\n\tat ...");

        DlqEnvelope envelope = reader.read(DLQ_TOPIC, 0, offset).orElseThrow();

        assertThat(envelope.dlqReason()).isEqualTo("STEP_TIMEOUT");
        assertThat(envelope.exceptionSummary())
                .isEqualTo("step aggregate deadline exceeded")   // first line ONLY — no stack trace
                .doesNotContain("at io.chaosforge");
        assertThat(envelope.originalTopic()).isEqualTo("chaosforge.scenario.commands.v1");
        assertThat(envelope.dlqAttempt()).isEqualTo(1);
        // Redaction by construction: the record carried a tenant id and an Avro payload; the
        // envelope type has nowhere to hold either.
        assertThat(envelope.toString()).doesNotContain("11111111-2222");
    }

    @Test
    void reader_missingOffset_isEmpty_andNonDlqTopic_isRejected() throws Exception {
        long offset = produceDlqRecord("SCHEMA_INVALID", "boom");

        assertThat(reader.read(DLQ_TOPIC, 0, offset + 500)).isEmpty();
        assertThatThrownBy(() -> reader.read("chaosforge.scenario.commands.v1", 0, 0L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void triageEndpoint_operatorAllowed_tenantForbidden_anonymousUnauthorized() throws Exception {
        long offset = produceDlqRecord("INFRA_TRANSIENT", "connect timed out");
        when(triageClient.advise(org.mockito.ArgumentMatchers.any())).thenReturn(new DlqTriageResult(
                "target likely unreachable", DlqTriageResult.SuggestedAction.REPLAY_WHEN_READY));

        mvc.perform(get("/v1/dlq/{topic}/triage/{offset}", DLQ_TOPIC, offset)
                        .param("partition", "0")
                        .header("Authorization", "Bearer " + OPERATOR_TOKEN))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.suggestedAction").value("REPLAY_WHEN_READY"));

        mvc.perform(get("/v1/dlq/{topic}/triage/{offset}", DLQ_TOPIC, offset)
                        .param("partition", "0")
                        .header("Authorization", "Bearer " + TENANT_TOKEN))
                .andExpect(status().isForbidden());

        mvc.perform(get("/v1/dlq/{topic}/triage/{offset}", DLQ_TOPIC, offset)
                        .param("partition", "0"))
                .andExpect(status().isUnauthorized());
    }

    /** Plants a realistic DLQ record (reason + DLT headers + tenant header + payload) and returns its offset. */
    private long produceDlqRecord(String reason, String exceptionMessage) throws Exception {
        ProducerRecord<String, byte[]> record =
                new ProducerRecord<>(DLQ_TOPIC, 0, "key", "avro-payload-bytes".getBytes(UTF_8));
        record.headers()
                .add(new RecordHeader("x-dlq-reason", reason.getBytes(UTF_8)))
                .add(new RecordHeader("x-dlq-attempt", "1".getBytes(UTF_8)))
                .add(new RecordHeader("x-tenant-id", "11111111-2222-3333-4444-555555555555".getBytes(UTF_8)))
                .add(new RecordHeader("kafka_dlt-exception-message", exceptionMessage.getBytes(UTF_8)))
                .add(new RecordHeader("kafka_dlt-original-topic",
                        "chaosforge.scenario.commands.v1".getBytes(UTF_8)));
        return outboxKafkaTemplate.send(record).get().getRecordMetadata().offset();
    }

    private static Jwt jwtWithRoles(List<String> roles) {
        return Jwt.withTokenValue("t")
                .header("alg", "RS256")
                .issuedAt(Instant.now())
                .expiresAt(Instant.now().plusSeconds(300))
                .claim("tenant_id", UUID.randomUUID().toString())
                .claim("roles", roles)
                .build();
    }
}
