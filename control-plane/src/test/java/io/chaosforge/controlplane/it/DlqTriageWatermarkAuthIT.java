package io.chaosforge.controlplane.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

import io.chaosforge.controlplane.dlq.DlqTriageWatermarkDao;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.context.WebApplicationContext;

/**
 * The OPERATOR-gated DLQ triage-watermark write endpoint (ADR-0542, module 3) — the point at which
 * "triage becomes stateful" reaches the HTTP surface. Proves, through the real Security filter chain
 * and against real Postgres:
 *
 * <ul>
 *   <li><b>Authorization</b> (ADR-0536): OPERATOR → 200 <em>and the watermark actually advances</em>
 *       (audited to the JWT subject); a plain tenant token → 403 <em>with no write</em>; no token →
 *       401 <em>with no write</em>. Authentication is not authorization; a denied request must not
 *       mutate.</li>
 *   <li><b>Input guard</b>: a non-{@code .DLQ} topic → 400 (this verb governs DLQ triage only).</li>
 * </ul>
 */
class DlqTriageWatermarkAuthIT extends AbstractCpIntegrationTest {

    private static final String DLQ_TOPIC = "watermark-auth-test.DLQ";   // own topic — no Kafka coupling
    private static final String OPERATOR_TOKEN = "operator.jwt.token";
    private static final String TENANT_TOKEN = "tenant.jwt.token";

    @MockitoBean
    private JwtDecoder jwtDecoder;

    @Autowired
    private DlqTriageWatermarkDao watermarks;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private WebApplicationContext wac;

    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = webAppContextSetup(wac).apply(springSecurity()).build();
        jdbc.update("TRUNCATE dlq_triage_watermark");
        when(jwtDecoder.decode(OPERATOR_TOKEN)).thenReturn(jwtWithSubjectAndRoles("operator-a", List.of("OPERATOR")));
        when(jwtDecoder.decode(TENANT_TOKEN)).thenReturn(jwtWithSubjectAndRoles("tenant-user", List.of("USER")));
    }

    @Test
    void operatorAdvancesWatermark_returns200_andPersistsAudited() throws Exception {
        mvc.perform(put("/v1/dlq/{topic}/reviewed", DLQ_TOPIC)
                        .param("partition", "0")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"reviewedOffset\":7}")
                        .header("Authorization", "Bearer " + OPERATOR_TOKEN))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.reviewedOffset").value(7))
                .andExpect(jsonPath("$.partition").value(0));

        assertThat(watermarks.reviewedOffset(DLQ_TOPIC, 0)).contains(7L);
        assertThat(reviewedBy(0)).isEqualTo("operator-a");   // audited to the VERIFIED JWT subject
    }

    @Test
    void tenantToken_isForbidden_andDoesNotWrite() throws Exception {
        mvc.perform(put("/v1/dlq/{topic}/reviewed", DLQ_TOPIC)
                        .param("partition", "0")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"reviewedOffset\":7}")
                        .header("Authorization", "Bearer " + TENANT_TOKEN))
                .andExpect(status().isForbidden());

        assertThat(watermarks.reviewedOffset(DLQ_TOPIC, 0)).isEmpty();   // 403 must not mutate
    }

    @Test
    void noToken_isUnauthorized_andDoesNotWrite() throws Exception {
        mvc.perform(put("/v1/dlq/{topic}/reviewed", DLQ_TOPIC)
                        .param("partition", "0")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"reviewedOffset\":7}"))
                .andExpect(status().isUnauthorized());

        assertThat(watermarks.reviewedOffset(DLQ_TOPIC, 0)).isEmpty();
    }

    @Test
    void nonDlqTopic_isRejected() throws Exception {
        mvc.perform(put("/v1/dlq/{topic}/reviewed", "chaosforge.scenario.commands.v1")
                        .param("partition", "0")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"reviewedOffset\":7}")
                        .header("Authorization", "Bearer " + OPERATOR_TOKEN))
                .andExpect(status().isBadRequest());
    }

    private String reviewedBy(int partition) {
        return jdbc.queryForObject(
                "SELECT reviewed_by FROM dlq_triage_watermark WHERE topic = ? AND partition = ?",
                String.class, DLQ_TOPIC, partition);
    }

    private static Jwt jwtWithSubjectAndRoles(String subject, List<String> roles) {
        return Jwt.withTokenValue("t")
                .header("alg", "RS256")
                .subject(subject)
                .issuedAt(Instant.now())
                .expiresAt(Instant.now().plusSeconds(300))
                .claim("tenant_id", UUID.randomUUID().toString())
                .claim("roles", roles)
                .build();
    }
}
