package io.chaosforge.controlplane.ai;

import jakarta.validation.ConstraintViolation;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The LLM draft failed the Bean Validation schema gate (ADR-0518). Mapped to {@code HTTP 422}. The
 * message lists violated <b>property paths only</b> — never field values (PII rule, ai-rules.md).
 */
public class AiOutputValidationException extends RuntimeException {
    public AiOutputValidationException(Set<? extends ConstraintViolation<?>> violations) {
        super(violations.size() + " AI-output constraint violation(s) at: "
                + violations.stream()
                        .map(v -> v.getPropertyPath().toString())
                        .sorted()
                        .collect(Collectors.joining(", ")));
    }
}
