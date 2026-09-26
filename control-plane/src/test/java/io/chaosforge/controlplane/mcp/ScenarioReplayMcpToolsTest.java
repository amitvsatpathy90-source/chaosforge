package io.chaosforge.controlplane.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.chaosforge.controlplane.error.ResourceNotFoundException;
import io.chaosforge.controlplane.replay.ConcurrentReplayException;
import io.chaosforge.controlplane.replay.IdempotencyKeyInProgressException;
import io.chaosforge.controlplane.replay.ReplayToken;
import io.chaosforge.controlplane.replay.ScenarioReplayOrchestrator;
import io.chaosforge.controlplane.security.TenantContext;

import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ScenarioReplayMcpToolsTest {

    private final ScenarioReplayOrchestrator orchestrator = mock(ScenarioReplayOrchestrator.class);
    private final ScenarioReplayMcpTools tools = new ScenarioReplayMcpTools(orchestrator);

    private final UUID scenarioId = UUID.randomUUID();
    private final UUID idemKey = UUID.randomUUID();

    @BeforeEach
    void bindTenant() {
        TenantContext.set(UUID.randomUUID());
    }

    @AfterEach
    void clearTenant() {
        TenantContext.clear();
    }

    @Test
    void freshClaim_delegatesAndReturnsToken() {
        ReplayToken token = ReplayToken.claimed(
                scenarioId, 1L, UUID.randomUUID(), 1, UUID.randomUUID());
        when(orchestrator.initiateReplay(any())).thenReturn(token);

        assertThat(tools.startScenario(scenarioId, 0L, idemKey)).isEqualTo(token);
    }

    @Test
    void resourceNotFound_propagatesUnchanged() {
        when(orchestrator.initiateReplay(any())).thenThrow(new ResourceNotFoundException(scenarioId));

        assertThatThrownBy(() -> tools.startScenario(scenarioId, 0L, idemKey))
                .isInstanceOf(ResourceNotFoundException.class);
    }

    @Test
    void concurrentReplay_propagatesUnchanged() {
        when(orchestrator.initiateReplay(any())).thenThrow(new ConcurrentReplayException(scenarioId, 0L));

        assertThatThrownBy(() -> tools.startScenario(scenarioId, 0L, idemKey))
                .isInstanceOf(ConcurrentReplayException.class);
    }

    @Test
    void idempotencyKeyInProgress_propagatesUnchanged() {
        when(orchestrator.initiateReplay(any()))
                .thenThrow(new IdempotencyKeyInProgressException(UUID.randomUUID(), idemKey));

        assertThatThrownBy(() -> tools.startScenario(scenarioId, 0L, idemKey))
                .isInstanceOf(IdempotencyKeyInProgressException.class);
    }

    @Test
    void negativeExpectedVersion_throwsIllegalArgumentException_beforeOrchestratorCall() {
        assertThatThrownBy(() -> tools.startScenario(scenarioId, -1L, idemKey))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(orchestrator);
    }

    @Test
    void unexpectedException_isMaskedWithGenericMessage() {
        when(orchestrator.initiateReplay(any())).thenThrow(new RuntimeException("sql: relation leaked"));

        assertThatThrownBy(() -> tools.startScenario(scenarioId, 0L, idemKey))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("internal error starting scenario replay")
                .satisfies(e -> assertThat(e.getMessage()).doesNotContain("relation"));
    }
}
