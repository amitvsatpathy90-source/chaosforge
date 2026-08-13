package io.chaosforge.execution.ruleset;

import java.util.Map;

/** A single fault-injection step parsed from a rule-set definition. */
public record StepSpec(
        String stepId,
        String stepType,        // LATENCY | ERROR | TIMEOUT
        String targetUrl,
        String method,
        Map<String, String> faultConfig) {
}
