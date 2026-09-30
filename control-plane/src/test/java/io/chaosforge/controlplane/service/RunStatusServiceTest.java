package io.chaosforge.controlplane.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.chaosforge.controlplane.domain.RunProjection;
import io.chaosforge.controlplane.repository.RunProjectionRepository;
import io.chaosforge.controlplane.security.TenantContext;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RunStatusServiceTest {

    private final ScenarioService scenarioService = mock(ScenarioService.class);
    private final RunProjectionRepository projectionRepository = mock(RunProjectionRepository.class);
    private final RunStatusService service = new RunStatusService(scenarioService, projectionRepository);

    private final UUID tenantId = UUID.randomUUID();
    private final UUID scenarioId = UUID.randomUUID();

    @BeforeEach
    void bindTenant() {
        TenantContext.set(tenantId);
    }

    @AfterEach
    void clearTenant() {
        TenantContext.clear();
    }

    @Test
    void terminalProjectionExists_returnsTerminalStatus() {
        when(scenarioService.replayVersion(scenarioId)).thenReturn(2L);
        Instant finishedAt = Instant.now();
        when(projectionRepository.findByScenarioIdAndReplayVersionAndTenantId(scenarioId, 2L, tenantId))
                .thenReturn(Optional.of(new RunProjection(scenarioId, 2L, tenantId, "COMPLETED", finishedAt, finishedAt)));

        RunStatusService.RunStatus status = service.getStatus(scenarioId);

        assertThat(status.status()).isEqualTo("TERMINAL");
        assertThat(status.outcome()).isEqualTo("COMPLETED");
        assertThat(status.finishedAt()).isEqualTo(finishedAt);
        assertThat(status.replayVersion()).isEqualTo(2L);
    }

    @Test
    void noProjectionAtCurrentVersion_returnsInProgress() {
        when(scenarioService.replayVersion(scenarioId)).thenReturn(1L);
        when(projectionRepository.findByScenarioIdAndReplayVersionAndTenantId(any(), any(Long.class), any()))
                .thenReturn(Optional.empty());

        RunStatusService.RunStatus status = service.getStatus(scenarioId);

        assertThat(status.status()).isEqualTo("IN_PROGRESS");
        assertThat(status.outcome()).isNull();
        assertThat(status.finishedAt()).isNull();
    }

    @Test
    void scenarioServiceGet_isCalled_toEnforceTenantScopedExistence() {
        when(scenarioService.replayVersion(scenarioId)).thenReturn(1L);
        when(projectionRepository.findByScenarioIdAndReplayVersionAndTenantId(any(), any(Long.class), any()))
                .thenReturn(Optional.empty());

        service.getStatus(scenarioId);

        org.mockito.Mockito.verify(scenarioService).get(scenarioId);
    }
}
