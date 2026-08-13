package io.chaosforge.controlplane.service;

import com.github.f4b6a3.uuid.UuidCreator;
import io.chaosforge.common.target.TargetNotAllowedException;
import io.chaosforge.common.target.TargetUrlGuard;
import io.chaosforge.controlplane.ai.TargetNotOwnedException;
import io.chaosforge.controlplane.domain.RuleSet;
import io.chaosforge.controlplane.error.ResourceNotFoundException;
import io.chaosforge.controlplane.repository.RuleSetRepository;
import io.chaosforge.controlplane.security.TenantContext;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Service
public class RuleSetService {

    private final RuleSetRepository repository;
    private final TargetUrlGuard targetUrlGuard;
    private final ObjectMapper objectMapper;

    public RuleSetService(RuleSetRepository repository, TargetUrlGuard targetUrlGuard,
                          ObjectMapper objectMapper) {
        this.repository = repository;
        this.targetUrlGuard = targetUrlGuard;
        this.objectMapper = objectMapper;
    }

    /** Creates a new append-only rule set at version 1 (ADR-0503). */
    public RuleSet create(String name, String definition) {
        UUID tenantId = TenantContext.require();
        // Reject unsafe targets at authoring (arch-audit HIGH-2) — 422 here, not STEP_FAILED at runtime.
        validateTargets(definition);
        UUID ruleSetId = UuidCreator.getTimeOrderedEpoch();
        repository.insert(ruleSetId, 1, tenantId, name, definition);
        return repository.findByRuleSetIdAndVersionAndTenantId(ruleSetId, 1, tenantId).orElseThrow();
    }

    /** Best-effort: pull every {@code targetUrl} out of the definition JSON and guard it. A definition
     *  that does not parse as JSON has no URLs to check here — the executor's guard is the hard backstop.
     *  A blocked target → {@link TargetNotOwnedException} → HTTP 422 (shape token only, never the URL). */
    private void validateTargets(String definition) {
        List<String> targetUrls = new ArrayList<>();
        try {
            JsonNode root = objectMapper.readTree(definition);
            root.findValuesAsString("targetUrl").forEach(targetUrls::add);
        } catch (JacksonException e) {
            return;   // not step JSON — nothing to validate at this boundary
        }
        try {
            targetUrlGuard.validateAll(targetUrls);
        } catch (TargetNotAllowedException e) {
            throw new TargetNotOwnedException(e.reason());
        }
    }

    public RuleSet get(UUID ruleSetId, int version) {
        UUID tenantId = TenantContext.require();
        return repository.findByRuleSetIdAndVersionAndTenantId(ruleSetId, version, tenantId)
                .orElseThrow(() -> new ResourceNotFoundException(ruleSetId));
    }

    /**
     * Internal (mTLS) path: returns the raw definition JSON for the Execution Service's rule-set
     * loader. Still tenant-scoped — {@code tenantId} comes from the consumer's verified Avro payload,
     * and a non-owning tuple yields 404 (ADR-0510). Not driven by {@link TenantContext}.
     */
    public String getDefinition(UUID ruleSetId, int version, UUID tenantId) {
        return repository.findByRuleSetIdAndVersionAndTenantId(ruleSetId, version, tenantId)
                .map(RuleSet::definition)
                .orElseThrow(() -> new ResourceNotFoundException(ruleSetId));
    }
}
