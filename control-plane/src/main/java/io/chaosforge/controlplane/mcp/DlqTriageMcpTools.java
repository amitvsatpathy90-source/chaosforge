package io.chaosforge.controlplane.mcp;

import io.chaosforge.controlplane.ai.AiUnavailableException;
import io.chaosforge.controlplane.ai.DlqTriageResult;
import io.chaosforge.controlplane.ai.DlqTriageService;
import io.chaosforge.controlplane.error.ResourceNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

/**
 * MCP wrapper over the existing read-only advisory DLQ triage surface (ADR-0518).
 * OPERATOR-gated, NOT tenant-scoped — DLQ records are cross-tenant by design (ADR-0536); this tool
 * never touches TenantContext. Watermark-advance stays HTTP-only — see DlqTriageWatermarkController:
 * the AI/tool surface must never write triage state.
 */
@Component
public class DlqTriageMcpTools {

    private static final Logger log = LoggerFactory.getLogger(DlqTriageMcpTools.class);

    private final DlqTriageService triageService;

    public DlqTriageMcpTools(DlqTriageService triageService) {
        this.triageService = triageService;
    }

    @PreAuthorize("hasRole('OPERATOR') and hasAuthority('SCOPE_chaosforge.dlq')")
    @McpTool(
            name = "get_dlq_triage",
            description = "Advisory triage for one DLQ record: hypothesis + suggested action "
                    + "(DISCARD|INVESTIGATE|REPLAY_WHEN_READY). Read-only — never replays, acks, "
                    + "or mutates state. OPERATOR role AND chaosforge.dlq scope required; "
                    + "DLQ records are cross-tenant.",
            generateOutputSchema = true,
            annotations = @McpTool.McpAnnotations(
                    readOnlyHint = true,
                    destructiveHint = false,
                    openWorldHint = true))   // calls out to local Ollama inference
    public DlqTriageResult getDlqTriage(
            @McpToolParam(description = "DLQ topic name, must end in .DLQ.", required = true)
            String topic,
            @McpToolParam(description = "Kafka partition.", required = true)
            int partition,
            @McpToolParam(description = "Record offset within the partition.", required = true)
            long offset) {
        try {
            return triageService.triage(topic, partition, offset);
        } catch (ResourceNotFoundException | IllegalArgumentException | AiUnavailableException e) {
            throw e;   // all three are safe, actionable, non-leaking — same contract as the HTTP endpoint
        } catch (RuntimeException e) {
            log.error("get_dlq_triage failed for topic={} partition={} offset={}", topic, partition, offset, e);
            throw new IllegalStateException("internal error retrieving dlq triage");
        }
    }
}
