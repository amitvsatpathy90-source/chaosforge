package io.chaosforge.controlplane.mcp;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.chaosforge.controlplane.error.ResourceNotFoundException;

import java.time.Instant;
import java.util.UUID;

import io.chaosforge.controlplane.service.RunStatusService;
import io.chaosforge.controlplane.service.RunStatusService.RunStatus;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

@Component
public class RunStatusMcpTools {

    private static final Logger log = LoggerFactory.getLogger(RunStatusMcpTools.class);

    private final RunStatusService runStatusService;

    public RunStatusMcpTools(RunStatusService runStatusService) {
        this.runStatusService = runStatusService;
    }

    @PreAuthorize("hasAuthority('SCOPE_chaosforge.read')")
    @McpTool(
            name = "get_run_status",
            description = "Get the current run status (latest replay) of a scenario for the authenticated tenant.",
            generateOutputSchema = true,
            annotations = @McpTool.McpAnnotations(
                    readOnlyHint = true,
                    destructiveHint = false,
                    openWorldHint = false))
    public RunStatusResponse getRunStatus(
            @McpToolParam(description = "Scenario UUID to check.", required = true)
            UUID scenarioId) {
        try {
            RunStatus status = runStatusService.getStatus(scenarioId);
            return new RunStatusResponse(status.scenarioId(), status.tenantId(), status.replayVersion(),
                    status.status(), status.outcome(), status.finishedAt());
        } catch (ResourceNotFoundException e) {
            throw e;   // ADR-0510 indistinguishability — same contract as get_scenario
        } catch (RuntimeException e) {
            log.error("get_run_status failed for scenarioId={}", scenarioId, e);
            throw new IllegalStateException("internal error retrieving run status");
        }
    }

    record RunStatusResponse(
            UUID scenarioId,
            UUID tenantId,
            long replayVersion,
            String status,
            @Nullable @JsonInclude(JsonInclude.Include.NON_NULL) String outcome,
            @Nullable @JsonInclude(JsonInclude.Include.NON_NULL) Instant finishedAt) {}
}
