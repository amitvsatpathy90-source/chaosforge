package io.chaosforge.controlplane.pipeline;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class RunProjectionMetrics {

    private final Counter decodeFailures;

    public RunProjectionMetrics(MeterRegistry registry) {
        decodeFailures = Counter.builder("chaosforge.run_projection.decode_failures")
                .description("ScenarioRunResult records that failed decode and were skipped, not DLQ-routed")
                .register(registry);
    }

    public void decodeFailure() {
        decodeFailures.increment();
    }
}
