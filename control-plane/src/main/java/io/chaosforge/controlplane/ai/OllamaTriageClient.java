package io.chaosforge.controlplane.ai;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.resilience4j.bulkhead.annotation.Bulkhead;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

/**
 * Ollama DLQ-triage boundary (ai-rules.md Tier 2 — advisory only). Own bean so reader faults
 * (400/404) never count against the shared ollama-chat CB. Prompt built only from redacted
 * {@link DlqEnvelope} fields (ADR-0519); exceptionSummary is attacker-influenceable free text
 * (GAP-06) so it's sanitized via {@link PromptSanitizer} before the user() slot.
 */
@Component
public class OllamaTriageClient {

    private static final Logger log = LoggerFactory.getLogger(OllamaTriageClient.class);
    private static final String SYSTEM_PROMPT = "prompts/dlq-triage-system.txt";

    private final ChatClient chatClient;
    private final PromptSanitizer sanitizer;
    private final String model;

    public OllamaTriageClient(ChatClient authoringChatClient, PromptSanitizer sanitizer,
                              @Value("${spring.ai.ollama.chat.options.model:llama3.1:8b}") String model) {
        this.chatClient = authoringChatClient;
        this.sanitizer = sanitizer;
        this.model = model;
    }

    @CircuitBreaker(name = "ollama-chat", fallbackMethod = "fallback")
    @Bulkhead(name = "ollama-chat")
    public DlqTriageResult advise(DlqEnvelope envelope) {
        // dlq_reason is a shape token (dlq-rules.md), safe to log; the prompt itself never is.
        log.info("ai triage request model=ollama/{} dlq_reason={}", model, envelope.dlqReason());
        return chatClient.prompt()
                .system(loadSystemPrompt())
                .user(buildPrompt(envelope))
                .call()
                .entity(DlqTriageResult.class);   // BeanOutputConverter; human decides on the output
    }

    // Package-private for OllamaTriageClientTest — the sanitization of the free-text field is the point.
    String buildPrompt(DlqEnvelope e) {
        return "dlq_reason: " + e.dlqReason() + '\n'
                + "original_topic: " + e.originalTopic() + '\n'
                + "retry_attempts_so_far: " + e.dlqAttempt() + '\n'
                // Free-text field (GAP-06) — sanitize before it enters the user() slot.
                + "exception_summary: " + sanitizer.sanitize(e.exceptionSummary());
    }

    /** CB open / bulkhead full / model call or output conversion failed — triage manually. */
    @SuppressWarnings("unused")
    private DlqTriageResult fallback(DlqEnvelope envelope, Throwable t) {
        log.warn("AI triage unavailable: {}", t.toString());   // never the prompt
        throw new AiUnavailableException("AI triage unavailable — triage manually");
    }

    private static String loadSystemPrompt() {
        try {
            return new ClassPathResource(SYSTEM_PROMPT).getContentAsString(UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("missing system prompt resource: " + SYSTEM_PROMPT, e);
        }
    }
}
