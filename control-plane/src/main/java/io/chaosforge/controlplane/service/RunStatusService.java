package io.chaosforge.controlplane.service;

import io.chaosforge.controlplane.domain.RunProjection;
import io.chaosforge.controlplane.repository.RunProjectionRepository;
import io.chaosforge.controlplane.security.TenantContext;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;

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
