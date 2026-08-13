package io.chaosforge.controlplane.web;

import io.chaosforge.controlplane.domain.RuleSet;
import io.chaosforge.controlplane.service.RuleSetService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/rule-sets")
public class RuleSetController {

    private final RuleSetService ruleSetService;

    public RuleSetController(RuleSetService ruleSetService) {
        this.ruleSetService = ruleSetService;
    }

    @PostMapping
    public ResponseEntity<RuleSetResponse> create(@Valid @RequestBody CreateRuleSetRequest req) {
        RuleSet rs = ruleSetService.create(req.name(), req.definition());
        return ResponseEntity.status(HttpStatus.CREATED).body(toResponse(rs));
    }

    @GetMapping("/{ruleSetId}/versions/{version}")
    public RuleSetResponse get(@PathVariable UUID ruleSetId, @PathVariable int version) {
        return toResponse(ruleSetService.get(ruleSetId, version));
    }

    private static RuleSetResponse toResponse(RuleSet rs) {
        return new RuleSetResponse(rs.ruleSetId(), rs.version(), rs.name(), rs.definition());
    }

    public record CreateRuleSetRequest(@NotBlank String name, @NotBlank String definition) {}

    public record RuleSetResponse(UUID ruleSetId, int version, String name, String definition) {}
}
