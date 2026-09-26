package io.chaosforge.controlplane.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.chaosforge.controlplane.domain.RuleSet;
import io.chaosforge.controlplane.error.ResourceNotFoundException;
import io.chaosforge.controlplane.service.RuleSetService;
import io.chaosforge.controlplane.service.RuleSetService.RuleSetPage;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

/** Error mapping and payload shape for {@link RuleSetMcpTools}. */
class RuleSetMcpToolsTest {

    private final RuleSetService service = mock(RuleSetService.class);
    private final RuleSetMcpTools tools = new RuleSetMcpTools(service);

    @Test
    void get_notFound_propagatesUnchanged() {
        UUID id = UUID.randomUUID();
        when(service.get(id, 1)).thenThrow(new ResourceNotFoundException(id));

        assertThatThrownBy(() -> tools.getRuleSet(id, 1))
                .isInstanceOf(ResourceNotFoundException.class);
    }

    @Test
    void get_unexpectedException_isMasked() {
        UUID id = UUID.randomUUID();
        when(service.get(id, 1)).thenThrow(new RuntimeException("column secret_column leaked"));

        assertThatThrownBy(() -> tools.getRuleSet(id, 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("internal error retrieving rule set");
    }

    @Test
    void get_missingVersion_isRejected() {
        assertThatThrownBy(() -> tools.getRuleSet(UUID.randomUUID(), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("version is required");
    }

    @Test
    void get_oversizedDefinition_isRejectedWithStaticMessage() {
        UUID id = UUID.randomUUID();
        String big = "x".repeat(RuleSetMcpTools.MAX_DEFINITION_CHARS + 1);
        when(service.get(id, 1)).thenReturn(new RuleSet(id, 1, UUID.randomUUID(), "rs", big, Instant.now()));

        assertThatThrownBy(() -> tools.getRuleSet(id, 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("definition exceeds MCP size limit");
    }

    @Test
    void get_happyPath_echoesResolvedVersion() {
        UUID id = UUID.randomUUID();
        when(service.get(id, 2)).thenReturn(new RuleSet(id, 2, UUID.randomUUID(), "rs", "{}", Instant.now()));

        RuleSetMcpTools.RuleSetResponse r = tools.getRuleSet(id, 2);

        assertThat(r.version()).isEqualTo(2);
        assertThat(r.definition()).isEqualTo("{}");
    }

    @Test
    void list_lastPage_omitsNextCursorFromJson() {
        when(service.list(10, null)).thenReturn(new RuleSetPage(List.of(), null));

        String json = new ObjectMapper().writeValueAsString(tools.listRuleSets(10, null));

        assertThat(json).doesNotContain("\"nextCursor\"");
    }

    @Test
    void list_badCursor_propagatesUnmasked() {
        when(service.list(10, "garbage")).thenThrow(new IllegalArgumentException("invalid cursor"));

        assertThatThrownBy(() -> tools.listRuleSets(10, "garbage"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid cursor");
    }

    @Test
    void list_unexpectedException_isMasked() {
        when(service.list(10, null)).thenThrow(new RuntimeException("column secret_column leaked"));

        assertThatThrownBy(() -> tools.listRuleSets(10, null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("internal error listing rule sets");
    }
}
