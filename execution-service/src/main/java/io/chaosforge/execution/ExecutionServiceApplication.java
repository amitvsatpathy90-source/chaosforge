package io.chaosforge.execution;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Execution Service — Spring MVC + Java 21 virtual threads + {@code @KafkaListener} (ADR-0500).
 * The consumer pipeline order is load-bearing: Avro+tenant → fencing → inbox → rule-set → steps →
 * outbox → manual ack. No reactive code; no Spring AI (ADR-0518).
 */
@SpringBootApplication
@EnableScheduling
public class ExecutionServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ExecutionServiceApplication.class, args);
    }
}
