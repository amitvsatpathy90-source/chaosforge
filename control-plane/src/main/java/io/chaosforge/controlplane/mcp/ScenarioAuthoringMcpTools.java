package io.chaosforge.controlplane.mcp;

import io.chaosforge.controlplane.ai.AiOutputValidationException;
import io.chaosforge.controlplane.ai.AiUnavailableException;
import io.chaosforge.controlplane.ai.ScenarioAuthoringService;
import io.chaosforge.controlplane.ai.ScenarioDraft;
import io.chaosforge.controlplane.ai.ScenarioDraftRequest;
import io.chaosforge.controlplane.ai.TargetNotOwnedException;
import io.chaosforge.controlplane.security.TenantContext;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

/**
 * MCP wrapper over the Tier-1 AI authoring path (ADR-0518). Returns a validated ScenarioDraft for
 * human review — never persists, matches the HTTP /v1/ai/scenario-drafts contract exactly. Gated on
 * chaosforge.operate, not chaosforge.read: this spends Ollama cycles, which is more than a lookup
 * even though nothing is written to the DB.
 */
@Component
public class ScenarioAuthoringMcpTools {

    private static final Logger log = LoggerFactory.getLogger(ScenarioAuthoringMcpTools.class);

    private final ScenarioAuthoringService authoringService;
    private final Validator validator;

    public ScenarioAuthoringMcpTools(ScenarioAuthoringService authoringService, Validator validator) {
        this.authoringService = authoringService;
        this.validator = validator;
    }

    @PreAuthorize("hasAuthority('SCOPE_chaosforge.operate')")
    @McpTool(
            name = "draft_scenario",
            description = "Generate a validated chaos-scenario draft from a natural-language description, "
                    + "for human review. Never persists — commit via the normal scenario/rule-set CRUD "
                    + "tools/endpoints as a separate explicit step.",
            generateOutputSchema = true,
            annotations = @McpTool.McpAnnotations(
                    readOnlyHint = true,   // nothing persisted, consistent with get_dlq_triage's reasoning
                    destructiveHint = false,
                    openWorldHint = true))
    public ScenarioDraft draftScenario(
            @McpToolParam(description = "Natural-language description of the desired chaos scenario, max 4000 chars.",
                    required = true)
            String description) {
        ScenarioDraftRequest request = new ScenarioDraftRequest(description);
        Set<ConstraintViolation<ScenarioDraftRequest>> violations = validator.validate(request);
        if (!violations.isEmpty()) {
            throw new IllegalArgumentException(violations.size() + " request constraint violation(s) at: "
                    + violations.stream()
                    .map(v -> v.getPropertyPath().toString())
                    .sorted()
                    .reduce((a, b) -> a + ", " + b)
                    .orElse(""));
        }
        try {
            return authoringService.generateDraft(request, TenantContext.require());
        } catch (AiUnavailableException | AiOutputValidationException | TargetNotOwnedException e) {
            throw e;   // safe, actionable, property-path/shape-token-only messages (PII rule)
        } catch (RuntimeException e) {
            log.error("draft_scenario failed", e);
            throw new IllegalStateException("internal error generating scenario draft");
        }
    }
}
