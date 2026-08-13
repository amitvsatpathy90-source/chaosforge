package io.chaosforge.controlplane.ai;

import org.springframework.stereotype.Component;

/**
 * Minimal hygiene on user-supplied authoring text before it enters the {@code user()} slot: strip
 * control characters and cap length. This is input discipline, not PII redaction.
 *
 * <p><b>Deferred:</b> the cloud opt-in profile's {@code CloudSafePromptBuilder} (ADR-0519) that
 * redacts tenant PII before sending text to a hosted model. The lab default uses local Ollama, so no
 * prompt leaves the host; the redacting builder lands with {@code application-cloud.yml}.
 */
@Component
public class PromptSanitizer {

    private static final int MAX_LEN = 4000;

    public String sanitize(String description) {
        if (description == null) {
            return "";
        }
        // Drop ASCII/Unicode control chars (keep normal whitespace) to avoid prompt-control injection.
        String cleaned = description.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", "").strip();
        return cleaned.length() > MAX_LEN ? cleaned.substring(0, MAX_LEN) : cleaned;
    }
}
