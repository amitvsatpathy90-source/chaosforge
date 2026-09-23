package io.chaosforge.controlplane.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.chaosforge.controlplane.error.ResourceNotFoundException;
import io.chaosforge.controlplane.service.ScenarioService;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** MCP-facing error mapping for {@link ScenarioMcpTools#getScenario}. */
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

    /** An internal failure must not leak its raw message through the MCP in-band error channel. */
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
}
