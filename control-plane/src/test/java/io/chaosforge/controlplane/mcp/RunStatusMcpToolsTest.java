package io.chaosforge.controlplane.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.chaosforge.controlplane.error.ResourceNotFoundException;
import io.chaosforge.controlplane.service.RunStatusService;
import io.chaosforge.controlplane.service.RunStatusService.RunStatus;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

/** MCP-facing error mapping and serialization for {@link RunStatusMcpTools#getRunStatus}. */
class RunStatusMcpToolsTest {

    private final RunStatusService runStatusService = mock(RunStatusService.class);
    private final RunStatusMcpTools tools = new RunStatusMcpTools(runStatusService);

    @Test
    void resourceNotFoundException_propagatesUnchanged() {
        UUID id = UUID.randomUUID();
        when(runStatusService.getStatus(id)).thenThrow(new ResourceNotFoundException(id));

        assertThatThrownBy(() -> tools.getRunStatus(id))
                .isInstanceOf(ResourceNotFoundException.class)
                .hasMessage("resource not found: " + id);
    }

    @Test
    void unexpectedException_isMaskedWithGenericMessage() {
        UUID id = UUID.randomUUID();
        when(runStatusService.getStatus(id))
                .thenThrow(new RuntimeException("relation \"run_projection\" column secret_column leaked"));

        assertThatThrownBy(() -> tools.getRunStatus(id))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("internal error retrieving run status")
                .satisfies(e -> assertThat(
                        e.getMessage()).doesNotContain("secret_column"));
    }

    @Test
    void terminalStatus_mapsAllFields() {
        UUID id = UUID.randomUUID();
        UUID tenantId = UUID.randomUUID();
        Instant finishedAt = Instant.now();
        when(runStatusService.getStatus(id))
                .thenReturn(new RunStatus(id, tenantId, 3L,
                        "TERMINAL", "COMPLETED", finishedAt));

        RunStatusMcpTools.RunStatusResponse response = tools.getRunStatus(id);

        assertThat(response.scenarioId()).isEqualTo(id);
        assertThat(response.tenantId()).isEqualTo(tenantId);
        assertThat(response.replayVersion()).isEqualTo(3L);
        assertThat(response.status()).isEqualTo("TERMINAL");
        assertThat(response.outcome()).isEqualTo("COMPLETED");
        assertThat(response.finishedAt()).isEqualTo(finishedAt);
    }

    // Proves null outcome/finishedAt are omitted from the MCP structured payload, not serialized as "null".
    @Test
    void inProgressStatus_omitsOutcomeAndFinishedAtFromSerializedJson() {
        UUID id = UUID.randomUUID();
        when(runStatusService.getStatus(id))
                .thenReturn(new RunStatus(id, UUID.randomUUID(), 0L,
                        "IN_PROGRESS", null, null));

        RunStatusMcpTools.RunStatusResponse response = tools.getRunStatus(id);
        String json = new ObjectMapper().writeValueAsString(response);

        assertThat(response.status()).isEqualTo("IN_PROGRESS");
        assertThat(json).doesNotContain("\"outcome\"").doesNotContain("\"finishedAt\"");
    }
}
