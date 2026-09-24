package io.chaosforge.controlplane.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

import io.chaosforge.controlplane.domain.Scenario;
import io.chaosforge.controlplane.error.ResourceNotFoundException;
import io.chaosforge.controlplane.service.ScenarioService;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

/** MCP-facing error mapping for {@link ScenarioMcpTools#getScenario} and {@link ScenarioMcpTools#listScenarios}. */
class ScenarioMcpToolsTest {

    private final ScenarioService scenarioService = mock(ScenarioService.class);
    private final ScenarioMcpTools tools = new ScenarioMcpTools(scenarioService);

    @Test
    void resourceNotFoundException_propagatesUnchanged() {
        UUID id = UUID.randomUUID();
        when(scenarioService.get(id)).thenThrow(new ResourceNotFoundException(id));

        assertThatThrownBy(() -> tools.getScenario(id))
                .isInstanceOf(ResourceNotFoundException.class)
                .hasMessage("resource not found: " + id);
    }

    // An internal failure must not leak its raw message through the MCP in-band error channel.
    @Test
    void unexpectedException_isMaskedWithGenericMessage() {
        UUID id = UUID.randomUUID();
        when(scenarioService.get(id))
                .thenThrow(new RuntimeException("relation \"scenarios\" column secret_column leaked"));

        assertThatThrownBy(() -> tools.getScenario(id))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("internal error retrieving scenario")
                .satisfies(e -> assertThat(e.getMessage()).doesNotContain("secret_column"));
    }

    @Test
    void listScenarios_happyPath_mapsPageToResult() {
        Scenario s = new Scenario(UUID.randomUUID(), UUID.randomUUID(), "sc",
                UUID.randomUUID(), 1, "PENDING", Instant.now(), Instant.now());
        ScenarioService.ScenarioPage page = new ScenarioService.ScenarioPage(List.of(s), "next-cursor-token");
        when(scenarioService.list(10, null)).thenReturn(page);

        ScenarioMcpTools.ScenarioListResult result = tools.listScenarios(10, null);

        assertThat(result.items()).hasSize(1);
        assertThat(result.items().getFirst().scenarioId()).isEqualTo(s.scenarioId());
        assertThat(result.nextCursor()).isEqualTo("next-cursor-token");
    }

    // Proves the null pagination cursor is omitted from the MCP structured payload.
    @Test
    void listScenarios_lastPage_omitsNextCursorFromSerializedJson() {
        when(scenarioService.list(10, null)).thenReturn(new ScenarioService.ScenarioPage(List.of(), null));

        ScenarioMcpTools.ScenarioListResult result = tools.listScenarios(10, null);

        String json = new ObjectMapper().writeValueAsString(result);
        assertThat(json).doesNotContain("\"nextCursor\"");
    }

    @Test
    void listScenarios_nullLimit_defersDefaultToService() {
        when(scenarioService.list(0, null))
                .thenReturn(new ScenarioService.ScenarioPage(List.of(), null));

        tools.listScenarios(null, null);

        verify(scenarioService).list(eq(0), eq((String) null));
    }

    /** A bad cursor is a client mistake, not an internal failure — must surface unmasked. */
    @Test
    void listScenarios_badCursor_propagatesUnmasked() {
        when(scenarioService.list(10, "garbage")).thenThrow(new IllegalArgumentException("invalid cursor"));

        assertThatThrownBy(() -> tools.listScenarios(10, "garbage"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid cursor");
    }

    /** Mirrors {@link #unexpectedException_isMaskedWithGenericMessage} for the list tool. */
    @Test
    void listScenarios_unexpectedException_isMaskedWithGenericMessage() {
        when(scenarioService.list(10, null))
                .thenThrow(new RuntimeException("relation \"scenarios\" column secret_column leaked"));

        assertThatThrownBy(() -> tools.listScenarios(10, null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("internal error listing scenarios")
                .satisfies(e -> assertThat(
                        e.getMessage()).doesNotContain("secret_column"));
    }
}
