package io.chaosforge.controlplane.ai;

/**
 * The Ollama authoring boundary is unavailable — circuit open, bulkhead full, or the model call
 * failed/timed out. Thrown by the Resilience4j fallback ({@code ollama-chat}). Mapped to
 * {@code HTTP 503}. There is no automatic re-prompt; the client authors manually (ai-rules.md).
 */
public class AiUnavailableException extends RuntimeException {
    public AiUnavailableException(String message) {
        super(message);
    }
}
