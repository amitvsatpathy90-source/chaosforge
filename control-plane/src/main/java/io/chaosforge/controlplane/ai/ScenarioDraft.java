package io.chaosforge.controlplane.ai;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.PositiveOrZero;
import java.util.List;
import java.util.Objects;

/**
 * Structured LLM authoring output (ADR-0518). The model fills this via {@code .entity(ScenarioDraft.class)}
 * (BeanOutputConverter). It is <b>never trusted on shape</b>: the Bean Validation constraints here are
 * the schema gate enforced in code, and {@code targetUrls()} feeds the tenant target-ownership check
 * — the real security control. A draft is returned to the client for review and is <b>never
 * auto-persisted</b>; committing requires a second explicit request.
 */
public record ScenarioDraft(
        @NotBlank String name,
        @NotBlank String summary,
        @NotEmpty @Valid List<StepDraft> steps) {

    /** Target URLs the draft would call — the input to the tenant-ownership / SSRF gate. */
    public List<String> targetUrls() {
        return steps == null ? List.of()
                : steps.stream().map(StepDraft::targetUrl).filter(Objects::nonNull).toList();
    }

    public record StepDraft(
            @NotBlank String stepId,
            @NotBlank String targetUrl,
            @NotBlank String method,
            @PositiveOrZero int delayMs) {
    }
}
