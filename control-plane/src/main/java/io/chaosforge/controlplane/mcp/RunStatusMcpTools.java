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

    /**
     * MCP wrapper over {@link RunStatusService} for the {@code get_run_status} tool (ADR-0543).
     * Read-only and tenant-scoped; a cross-tenant or unknown scenario id gives the same 404 as
     * {@code get_scenario} (ADR-0510).
     *
     * <p>Status comes from {@code run_projection}, a rebuildable read cache fed from terminal
     * {@code ScenarioRunResult} events. No terminal row for the scenario's current replay version
     * reports {@code IN_PROGRESS}, which also covers a scenario that has never run. A record the
     * projection gave up on stays stale until that scenario is replayed again.
     */
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
