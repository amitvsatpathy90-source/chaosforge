package io.chaosforge.controlplane.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.chaosforge.controlplane.ai.AiUnavailableException;
import io.chaosforge.controlplane.ai.DlqTriageResult;
import io.chaosforge.controlplane.ai.DlqTriageService;
import io.chaosforge.controlplane.error.ResourceNotFoundException;
import org.junit.jupiter.api.Test;

class DlqTriageMcpToolsTest {

    private final DlqTriageService triageService = mock(DlqTriageService.class);
    private final DlqTriageMcpTools tools = new DlqTriageMcpTools(triageService);

    @Test
    void delegatesAndReturnsResultUnchanged() {
        DlqTriageResult expected = new DlqTriageResult("target unreachable",
                DlqTriageResult.SuggestedAction.REPLAY_WHEN_READY);
        when(triageService.triage("t.DLQ", 0, 42L)).thenReturn(expected);

        assertThat(tools.getDlqTriage("t.DLQ", 0, 42L)).isEqualTo(expected);
    }

    @Test
    void resourceNotFound_propagatesUnchanged() {
        when(triageService.triage("t.DLQ", 0, 999L))
                .thenThrow(new ResourceNotFoundException("dlq record not found"));

        assertThatThrownBy(() -> tools.getDlqTriage("t.DLQ", 0, 999L))
                .isInstanceOf(ResourceNotFoundException.class);
    }

    @Test
    void illegalArgument_propagatesUnchanged() {
        when(triageService.triage("not-a-dlq-topic", 0, 0L))
                .thenThrow(new IllegalArgumentException("only .DLQ topics are readable by triage"));

        assertThatThrownBy(() -> tools.getDlqTriage("not-a-dlq-topic", 0, 0L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void unexpectedException_isMaskedWithGenericMessage() {
        when(triageService.triage("t.DLQ", 0, 1L))
                .thenThrow(new RuntimeException("ollama connection refused at 10.0.0.5:11434"));

        assertThatThrownBy(() -> tools.getDlqTriage("t.DLQ", 0, 1L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("internal error retrieving dlq triage")
                .satisfies(e -> assertThat(e.getMessage()).doesNotContain("10.0.0.5"));
    }

    @Test
    void aiUnavailable_propagatesUnchanged_notMasked() {
        when(triageService.triage("t.DLQ", 0, 1L))
                .thenThrow(new AiUnavailableException("AI triage unavailable — triage manually"));

        assertThatThrownBy(() -> tools.getDlqTriage("t.DLQ", 0, 1L))
                .isInstanceOf(AiUnavailableException.class)
                .hasMessage("AI triage unavailable — triage manually");
    }
}
