package io.chaosforge.controlplane.mcp;

import io.chaosforge.controlplane.error.ResourceNotFoundException;
import io.chaosforge.controlplane.replay.ConcurrentReplayException;
import io.chaosforge.controlplane.replay.IdempotencyKeyInProgressException;
import io.chaosforge.controlplane.replay.ReplayCommand;
import io.chaosforge.controlplane.replay.ReplayToken;
import io.chaosforge.controlplane.replay.ScenarioReplayOrchestrator;
import io.chaosforge.controlplane.security.TenantContext;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

/**
 * MCP wrapper over the replay critical section (ADR-0528). No prepare/pre-step: expectedVersion
 * comes from a prior get_scenario call's replayVersion; idempotencyKey is caller-generated and must
 * be reused across retries of the same logical attempt — minting a new one per call would silently
 * defeat the at-most-once retry guarantee. Batch 7 scope note: prepare_scenario_run dropped as
 * redundant with get_scenario; mark_dlq_reviewed excluded by design, stays HTTP-only.
 */
@Component
public class ScenarioReplayMcpTools {

    private static final Logger log = LoggerFactory.getLogger(ScenarioReplayMcpTools.class);

    private final ScenarioReplayOrchestrator orchestrator;

    public ScenarioReplayMcpTools(ScenarioReplayOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @PreAuthorize("hasAuthority('SCOPE_chaosforge.operate')")
    @McpTool(
            name = "start_scenario",
            description = "Initiate a scenario replay (ADR-0528). expectedVersion must come from a prior "
                    + "get_scenario call's replayVersion (optimistic concurrency, like an HTTP If-Match). "
                    + "idempotencyKey must be a UUID generated once per logical attempt and reused on any "
                    + "retry of that same attempt — a new UUID per call defeats the at-most-once guarantee.",
            generateOutputSchema = true,
            annotations = @McpTool.McpAnnotations(
                    readOnlyHint = false,
                    destructiveHint = false,
                    openWorldHint = false))
    public ReplayToken startScenario(
            @McpToolParam(description = "Scenario UUID to replay.", required = true)
            UUID scenarioId,
            @McpToolParam(description = "Last-observed replay version for this scenario (optimistic concurrency).",
                    required = true)
            long expectedVersion,
            @McpToolParam(description = "Caller-generated UUID, one per logical attempt; reuse on retry.",
                    required = true)
            UUID idempotencyKey) {
        try {
            ReplayCommand command = new ReplayCommand(scenarioId, TenantContext.require(), expectedVersion, idempotencyKey);
            return orchestrator.initiateReplay(command);
        } catch (ResourceNotFoundException | ConcurrentReplayException
                 | IdempotencyKeyInProgressException | IllegalArgumentException e) {
            throw e;   // safe, actionable, non-leaking — same contract as the HTTP endpoint's 404/409/400
        } catch (RuntimeException e) {
            log.error("start_scenario failed for scenarioId={}", scenarioId, e);
            throw new IllegalStateException("internal error starting scenario replay");
        }
    }
}
