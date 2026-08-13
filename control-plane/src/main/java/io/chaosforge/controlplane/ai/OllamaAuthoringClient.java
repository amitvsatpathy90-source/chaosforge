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
 * The Ollama authoring boundary, isolated in its own bean so the Resilience4j {@code ollama-chat}
 * CB + bulkhead actually apply (a self-invocation from {@link ScenarioAuthoringService} would bypass
 * the AOP proxy). Returns the <b>raw, unvalidated</b> draft — validation lives in the service, outside
 * this circuit, so a malformed draft is a 422, never an AI-unavailable fault.
 *
 * <p>The system prompt is a <b>static classpath resource</b>; user text goes only in {@code user()}.
 * Prompt content is never logged (PII surface) — only the model id is.
 *
 * <p>Timeout note: R4j {@code @TimeLimiter} is omitted — it requires a {@code CompletableFuture}
 * return and does not apply to this blocking virtual-thread call (same posture as the Execution
 * Service). The request is bounded by the Ollama client timeout; local CPU inference is slow by
 * design (disclose in benchmarks).
 */
@Component
public class OllamaAuthoringClient {

    private static final Logger log = LoggerFactory.getLogger(OllamaAuthoringClient.class);
    private static final String SYSTEM_PROMPT = "prompts/scenario-authoring-system.txt";

    private final ChatClient chatClient;
    private final PromptSanitizer sanitizer;
    private final String model;

    public OllamaAuthoringClient(ChatClient authoringChatClient, PromptSanitizer sanitizer,
                                 @Value("${spring.ai.ollama.chat.options.model:llama3.1:8b}") String model) {
        this.chatClient = authoringChatClient;
        this.sanitizer = sanitizer;
        this.model = model;
    }

    @CircuitBreaker(name = "ollama-chat", fallbackMethod = "fallback")
    @Bulkhead(name = "ollama-chat")
    public ScenarioDraft generate(ScenarioDraftRequest request) {
        log.info("ai authoring request model=ollama/{}", model);   // never log prompt content
        String safeDescription = sanitizer.sanitize(request.description());
        return chatClient.prompt()
                .system(loadSystemPrompt())
                .user(safeDescription)
                .call()
                .entity(ScenarioDraft.class);   // BeanOutputConverter; validated by the service
    }

    /** CB open / bulkhead full / model call failed. No auto re-prompt (ai-rules.md). */
    @SuppressWarnings("unused")
    private ScenarioDraft fallback(ScenarioDraftRequest request, Throwable t) {
        log.warn("AI authoring unavailable: {}", t.toString());   // never the prompt
        throw new AiUnavailableException("AI authoring unavailable — please author manually");
    }

    private static String loadSystemPrompt() {
        try {
            return new ClassPathResource(SYSTEM_PROMPT).getContentAsString(UTF_8);
        } catch (IOException e) {
            // Missing static prompt is a deploy error, not a model failure.
            throw new IllegalStateException("missing system prompt resource: " + SYSTEM_PROMPT, e);
        }
    }
}
