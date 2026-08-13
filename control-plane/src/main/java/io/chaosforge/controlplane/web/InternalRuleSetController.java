package io.chaosforge.controlplane.web;

import io.chaosforge.controlplane.service.RuleSetService;
import java.util.UUID;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Intra-service endpoint for the Execution Service's rule-set loader. mTLS-gated (the lab runs it
 * open — see mtls-rules.md). Returns the pinned rule set's definition JSON (a step array) verbatim;
 * {@code tenantId} is the consumer's verified Avro tenant and scopes the read (404 if not owned).
 */
@RestController
@RequestMapping("/internal/rule-sets")
public class InternalRuleSetController {

    private final RuleSetService ruleSetService;

    public InternalRuleSetController(RuleSetService ruleSetService) {
        this.ruleSetService = ruleSetService;
    }

    @GetMapping(value = "/{ruleSetId}/versions/{version}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String getDefinition(@PathVariable UUID ruleSetId, @PathVariable int version,
                                @RequestParam UUID tenantId) {
        return ruleSetService.getDefinition(ruleSetId, version, tenantId);
    }
}
