package io.chaosforge.controlplane.ai;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * GAP-06: the DLQ-triage prompt must sanitize its one free-text field ({@code exceptionSummary}) before
 * it reaches the model - control chars stripped, length capped - so an exception message that echoes a
 * tenant-supplied value can't smuggle prompt-control characters into the {@code user()} slot. The
 * structural tokens ({@code dlqReason} / {@code originalTopic}) are passed through unchanged.
 */
class OllamaTriageClientTest {

    private static final String BEL = String.valueOf((char) 0x07);   // control char to strip
    private static final String NUL = String.valueOf((char) 0x00);   // control char to strip
    private static final String ESC = String.valueOf((char) 0x1b);   // ANSI escape byte to strip

    // ChatClient is null: buildPrompt never touches it, and constructing a real ChatClient needs Ollama.
    private final OllamaTriageClient client = new OllamaTriageClient(null, new PromptSanitizer(), "llama3.1:8b");

    @Test
    void buildPrompt_stripsControlCharsFromExceptionSummary() {
        // An exception message carrying BEL, NUL, and an ANSI escape. The printable "[31m" tail stays;
        // only the ESC byte itself is a control char to be stripped.
        String rawSummary = "boom" + BEL + NUL + " injected" + ESC + "[31m";
        DlqEnvelope env = new DlqEnvelope(
                "INFRA_TRANSIENT", rawSummary, "chaosforge.scenario.commands.v1.DLQ", 2);

        String prompt = client.buildPrompt(env);

        assertThat(prompt)
                .as("control chars stripped from the free-text summary; printable text preserved")
                .contains("exception_summary: boom injected[31m")
                .doesNotContain(BEL)
                .doesNotContain(NUL)
                .doesNotContain(ESC);
        // Structural tokens flow through untouched.
        assertThat(prompt).contains("dlq_reason: INFRA_TRANSIENT");
        assertThat(prompt).contains("original_topic: chaosforge.scenario.commands.v1.DLQ");
        assertThat(prompt).contains("retry_attempts_so_far: 2");
    }
}
