package io.chaosforge.controlplane.config;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring AI wiring — Control Plane only (ADR-0518/0521). The {@code ChatClient.Builder} is
 * auto-configured from {@code spring.ai.ollama.*}; per-request system/user prompts and the
 * {@code .entity(...)} structured-output converter are applied at call time by {@code
 * OllamaAuthoringClient}. No {@code PromptChatMemoryAdvisor} — authoring is stateless (ai-rules.md).
 */
@Configuration
public class AiConfig {

    @Bean
    ChatClient authoringChatClient(ChatClient.Builder builder) {
        return builder.build();
    }
}
