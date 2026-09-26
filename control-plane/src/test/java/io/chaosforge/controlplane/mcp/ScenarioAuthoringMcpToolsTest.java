package io.chaosforge.controlplane.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.chaosforge.controlplane.ai.AiOutputValidationException;
import io.chaosforge.controlplane.ai.AiUnavailableException;
import io.chaosforge.controlplane.ai.ScenarioAuthoringService;
import io.chaosforge.controlplane.ai.ScenarioDraft;
import io.chaosforge.controlplane.ai.TargetNotOwnedException;
import io.chaosforge.controlplane.security.TenantContext;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ScenarioAuthoringMcpToolsTest {

    private final ScenarioAuthoringService authoringService = mock(ScenarioAuthoringService.class);
    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
    private final ScenarioAuthoringMcpTools tools = new ScenarioAuthoringMcpTools(authoringService, validator);

    @BeforeEach
    void bindTenant() {
        TenantContext.set(UUID.randomUUID());
    }

    @AfterEach
    void clearTenant() {
        TenantContext.clear();
    }

    @Test
    void validRequest_delegatesAndReturnsDraft() {
        ScenarioDraft draft = new ScenarioDraft("name", "summary",
                List.of(new ScenarioDraft.StepDraft(
                        "s1", "https://example.com", "GET", 0)));
        when(authoringService.generateDraft(any(), any())).thenReturn(draft);

        assertThat(tools.draftScenario("kill the payments pod for 30s")).isEqualTo(draft);
    }

    @Test
    void blankDescription_throwsIllegalArgumentException_withoutCallingService() {
        assertThatThrownBy(() -> tools.draftScenario(""))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(authoringService);
    }

    @Test
    void tooLongDescription_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> tools.draftScenario("a".repeat(4001)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aiUnavailable_propagatesUnchanged() {
        when(authoringService.generateDraft(any(), any()))
                .thenThrow(new AiUnavailableException("AI authoring unavailable — please author manually"));

        assertThatThrownBy(() -> tools.draftScenario("valid description"))
                .isInstanceOf(AiUnavailableException.class);
    }

    @Test
    void aiOutputValidationFailure_propagatesUnchanged() {
        when(authoringService.generateDraft(any(), any()))
                .thenThrow(new AiOutputValidationException(Set.of()));

        assertThatThrownBy(() -> tools.draftScenario("valid description"))
                .isInstanceOf(AiOutputValidationException.class);
    }

    @Test
    void targetNotOwned_propagatesUnchanged() {
        when(authoringService.generateDraft(any(), any()))
                .thenThrow(new TargetNotOwnedException("internal_host_blocked"));

        assertThatThrownBy(() -> tools.draftScenario("valid description"))
                .isInstanceOf(TargetNotOwnedException.class);
    }

    @Test
    void unexpectedException_isMaskedWithGenericMessage() {
        when(authoringService.generateDraft(any(), any()))
                .thenThrow(new RuntimeException("ollama internal stacktrace leaked"));

        assertThatThrownBy(() -> tools.draftScenario("valid description"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("internal error generating scenario draft")
                .satisfies(e -> assertThat(e.getMessage()).doesNotContain("stacktrace"));
    }
}
