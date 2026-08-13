package io.chaosforge.controlplane.web;

import io.chaosforge.controlplane.ai.DlqTriageResult;
import io.chaosforge.controlplane.ai.DlqTriageService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read-only advisory DLQ triage (ADR-0518 surface 2; ai-rules.md Tier 2). GET only — this endpoint
 * can never replay, ack, or mutate anything.
 *
 * <p><b>OPERATOR-gated</b> (SecurityConfig): DLQ records span all tenants, so a plain tenant token
 * must not read them — authentication is not authorization (ADR-0536). The {@code partition} param
 * is required because a record is addressed by (topic, partition, offset); the DLQ has as many
 * partitions as the command topic (same-index routing).
 */
@RestController
public class DlqTriageController {

    private final DlqTriageService triageService;

    public DlqTriageController(DlqTriageService triageService) {
        this.triageService = triageService;
    }

    @GetMapping("/v1/dlq/{topic}/triage/{offset}")
    public DlqTriageResult triage(@PathVariable String topic,
                                  @PathVariable long offset,
                                  @RequestParam int partition) {
        return triageService.triage(topic, partition, offset);
    }
}
