package io.chaosforge.controlplane.service;

import io.chaosforge.controlplane.domain.RunProjection;
import io.chaosforge.controlplane.repository.RunProjectionRepository;
import io.chaosforge.controlplane.security.TenantContext;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;

/**
 * Latest-replay run status for one scenario, read from the {@code run_projection} cache.
 * Used by the {@code get_run_status} MCP tool (ADR-0543).
 *
 * <p>The lookup is keyed on the scenario's current replay version and the caller's tenant.
 * A terminal row gives {@code TERMINAL} plus outcome and finish time. No row gives
 * {@code IN_PROGRESS}, because the projection holds terminal results only. Tenant-scoped
 * existence is checked first so that an unknown scenario never reads as {@code IN_PROGRESS}.
 */
@Service
public class RunStatusService {

    private final ScenarioService scenarioService;
    private final RunProjectionRepository runProjectionRepository;

    public RunStatusService(ScenarioService scenarioService, RunProjectionRepository runProjectionRepository) {
        this.scenarioService = scenarioService;
        this.runProjectionRepository = runProjectionRepository;
    }

    public RunStatus getStatus(UUID scenarioId) {
        // Confirms tenant-scoped existence first (ADR-0510) — a projection miss alone can't tell
        // "still running" apart from "wrong tenant / nonexistent scenario".
        scenarioService.get(scenarioId);
        long replayVersion = scenarioService.replayVersion(scenarioId);
        UUID tenantId = TenantContext.require();

        Optional<RunProjection> projection = runProjectionRepository
                .findByScenarioIdAndReplayVersionAndTenantId(scenarioId, replayVersion, tenantId);

        return projection
                .map(p -> new RunStatus(
                        scenarioId,
                        tenantId,
                        replayVersion,
                        "TERMINAL",
                        p.outcome(),
                        p.finishedAt()))
                .orElseGet(() -> new RunStatus(
                        scenarioId,
                        tenantId,
                        replayVersion,
                        "IN_PROGRESS",
                        null,
                        null));
    }

    public record RunStatus(UUID scenarioId, UUID tenantId, long replayVersion, String status,
                            String outcome, Instant finishedAt) {}
}
