package io.chaosforge.gateway.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * SRE-01 (grounded): with structured logging active — as the deployed {@code mtls} profile sets it via
 * {@code logging.structured.format.console: ecs} — the console emits ECS JSON, so {@code trace_id} /
 * {@code span_id} (Micrometer Brave MDC) and fields are first-class and log-pipeline-queryable rather
 * than interpolated into free text. The format is profile-independent (the mtls profile is only its
 * delivery vehicle), so it is activated here by the property directly, avoiding the keystores a real
 * mtls boot would need. Also proves {@code ecs} is a valid format name (an invalid one fails startup).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"logging.structured.format.console=ecs", "management.server.port=0"})
@Import(StructuredLoggingIT.WebClientBuilderConfig.class)
@ExtendWith(OutputCaptureExtension.class)
class StructuredLoggingIT {

    /** See GatewayActuatorExposureIT: supply the builder the test slice doesn't autoconfigure. */
    @TestConfiguration(proxyBeanMethods = false)
    static class WebClientBuilderConfig {
        @Bean
        WebClient.Builder webClientBuilder() {
            return WebClient.builder();
        }
    }

    @Test
    void consoleLogsAreEcsJson(CapturedOutput output) {
        // Startup logging already ran under the active formatter. ECS lines are single-line JSON objects
        // with NESTED fields: {"@timestamp":...,"log":{"level":...},"service":{"name":"edge-gateway"},
        // ...,"ecs":{"version":"8.11"}} — never the plaintext "  INFO  " pattern.
        assertThat(output.getOut())
                .as("structured (ECS JSON) console logging is active on the deployed profile")
                .contains("\"@timestamp\"")
                .contains("\"ecs\":{\"version\"")
                .contains("\"log\":{\"level\"")
                .contains("\"service\":{\"name\":\"edge-gateway\"");
    }
}
