package io.chaosforge.controlplane.mcp;

import io.chaosforge.controlplane.domain.Scenario;
import io.chaosforge.controlplane.error.ResourceNotFoundException;
import io.chaosforge.controlplane.service.ScenarioService;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

/**
 * MCP read capabilities for scenarios.
 *
 * <p>Authorization intentionally lives at the MCP capability boundary rather than in
 * {@link ScenarioService}. This keeps MCP-specific scope authorization isolated from
 * the shared domain service, allowing the MCP and REST transports to retain their
 * respective authorization contracts.
 *
 * <p>Tenant isolation remains owned by {@link ScenarioService}: it resolves the
 * authenticated tenant context and performs the tenant-scoped lookup.
 */
@Component
public class ScenarioMcpTools {

    private final ScenarioService scenarioService;

    private static final Logger log = LoggerFactory.getLogger(ScenarioMcpTools.class);

    public ScenarioMcpTools(ScenarioService scenarioService) {
        this.scenarioService = scenarioService;
    }

    /**
     * Read-only scenario lookup for MCP clients.
     *
     * <p>The MCP transport authenticates the request using the dedicated MCP audience.
     * This method then performs the finer-grained capability check using the allow-listed
     * SCOPE_chaosforge.read authority.
     */
    @PreAuthorize("hasAuthority('SCOPE_chaosforge.read')")
    @McpTool(
            name = "get_scenario",
            description = "Get a single ChaosForge scenario belonging to the authenticated tenant.",
            generateOutputSchema = true,
            annotations = @McpTool.McpAnnotations(
                    readOnlyHint = true,
                    destructiveHint = false,
                    openWorldHint = false))
    public ScenarioResponse getScenario(
            @McpToolParam(
                    description = "Scenario UUID to retrieve.",
                    required = true)
            UUID scenarioId) {
        try {
            Scenario s = scenarioService.get(scenarioId);
            long replayVersion = scenarioService.replayVersion(scenarioId);

            return new ScenarioResponse(
                    s.scenarioId(),
                    s.tenantId(),
                    s.name(),
                    s.ruleSetId(),
                    s.ruleSetVersion(),
                    s.status(),
                    replayVersion
            );
        } catch (ResourceNotFoundException e) {
            // Already proven safe/indistinguishable (ADR-0510, ScenarioMcpToolIsolationIT) —
            // let it surface via the MCP engine's default in-band wrapping unchanged.
            throw e;
        } catch (RuntimeException e) {
            // The MCP engine puts e.getMessage() verbatim in the client-visible response —
            // an unfiltered internal exception (SQL text, config) must not reach the client.
            log.error("get_scenario failed for scenarioId={}", scenarioId, e);
            throw new IllegalStateException("internal error retrieving scenario");
        }
    }

    /**
     * MCP-specific response DTO.
     *
     * <p>Do not reuse ScenarioController.ScenarioResponse here. MCP and REST are separate
     * transport contracts and should not become coupled merely because their payloads are
     * currently similar.
     */
    record ScenarioResponse(
            UUID scenarioId,
            UUID tenantId,
            String name,
            UUID ruleSetId,
            int ruleSetVersion,
            String status,
            long replayVersion) {
    }
}
