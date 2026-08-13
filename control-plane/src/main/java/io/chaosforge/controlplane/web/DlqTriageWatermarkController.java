package io.chaosforge.controlplane.web;

import io.chaosforge.controlplane.dlq.DlqReviewedRequest;
import io.chaosforge.controlplane.dlq.DlqTriageWatermarkDao;
import io.chaosforge.controlplane.dlq.DlqWatermark;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * OPERATOR-gated write surface for the DLQ human-triage watermark (ADR-0542) — <b>the one stateful
 * corner of DLQ triage</b>. It is kept deliberately off the read-only {@code DlqTriageService} /
 * {@code DlqRecordReader} (ai-rules.md, ADR-0518): the LLM advisory path stays strictly read-only, so
 * "the AI never writes state" remains literally true; only this explicit operator verb mutates.
 *
 * <p>Advancing the watermark asserts "a human reviewed up to here", which lowers
 * {@code chaosforge.dlq.untriaged_depth} — a control action, not a query. Hence it is:
 * <ul>
 *   <li><b>OPERATOR-gated</b> — {@code /v1/dlq/**} → {@code hasRole("OPERATOR")} (SecurityConfig,
 *       ADR-0536): DLQ metadata is cross-tenant, so a plain tenant token is 403.</li>
 *   <li><b>audited</b> — {@code reviewed_by} is the verified JWT subject, not a caller-supplied value.</li>
 *   <li><b>Kafka-inert</b> — it writes only the CP Postgres watermark row; it never commits a Kafka
 *       offset, acks, or replays a record. The retry consumer's group offset is untouched.</li>
 * </ul>
 */
@RestController
public class DlqTriageWatermarkController {

    private final DlqTriageWatermarkDao watermarks;

    public DlqTriageWatermarkController(DlqTriageWatermarkDao watermarks) {
        this.watermarks = watermarks;
    }

    @PutMapping("/v1/dlq/{topic}/reviewed")
    public DlqWatermark markReviewed(@PathVariable String topic,
                                     @RequestParam int partition,
                                     @RequestBody DlqReviewedRequest body,
                                     @AuthenticationPrincipal Jwt operator) {
        if (!topic.endsWith(".DLQ")) {
            // Same guard as DlqRecordReader: this endpoint governs DLQ triage only, never an arbitrary topic.
            throw new IllegalArgumentException("only .DLQ topics carry a triage watermark");
        }
        if (partition < 0 || body.reviewedOffset() < 0) {
            throw new IllegalArgumentException("partition and reviewedOffset must be non-negative");
        }
        long resulting = watermarks.advance(topic, partition, body.reviewedOffset(), reviewedBy(operator));
        return new DlqWatermark(topic, partition, resulting);
    }

    /** The verified JWT subject is the audit identity; fall back defensively so reviewed_by is never null. */
    private static String reviewedBy(Jwt operator) {
        String subject = operator == null ? null : operator.getSubject();
        return (subject == null || subject.isBlank()) ? "operator" : subject;
    }
}
