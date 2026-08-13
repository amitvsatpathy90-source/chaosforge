package io.chaosforge.controlplane.ai;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * The natural-language authoring request. {@code description} is the only user-supplied text and goes
 * into the {@code user()} slot of the prompt only — never the {@code system()} slot (ai-rules.md).
 */
public record ScenarioDraftRequest(
        @NotBlank @Size(max = 4000) String description) {
}
