package io.chaosforge.controlplane.web;

import io.chaosforge.controlplane.ai.ScenarioAuthoringService;
import io.chaosforge.controlplane.ai.ScenarioDraft;
import io.chaosforge.controlplane.ai.ScenarioDraftRequest;
import io.chaosforge.controlplane.security.TenantContext;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Tier-1 AI authoring endpoint (ADR-0518). Returns a validated {@link ScenarioDraft} for human
 * review — it does <b>not</b> persist anything. To commit, the client makes a separate explicit
 * call to the normal scenario/rule-set CRUD endpoints. Tenant identity comes from the verified JWT
 * ({@link TenantContext}); the AI path is entirely off the deterministic replay path.
 */
@RestController
@RequestMapping("/v1/ai")
public class ScenarioAuthoringController {

    private final ScenarioAuthoringService authoringService;

    public ScenarioAuthoringController(ScenarioAuthoringService authoringService) {
        this.authoringService = authoringService;
    }

    @PostMapping("/scenario-drafts")
    public ScenarioDraft draft(@Valid @RequestBody ScenarioDraftRequest request) {
        return authoringService.generateDraft(request, TenantContext.require());
    }
}
