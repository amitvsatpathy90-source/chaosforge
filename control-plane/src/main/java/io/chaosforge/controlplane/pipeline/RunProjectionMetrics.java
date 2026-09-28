package io.chaosforge.controlplane.pipeline;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class RunProjectionMetrics {

    private final Counter decodeFailures;
    private final Counter persistFailures;
    private final Counter persistFailuresExhausted;

    public RunProjectionMetrics(MeterRegistry registry) {
        decodeFailures = Counter.builder("chaosforge.run_projection.decode_failures")
                .description("ScenarioRunResult records that failed decode and were skipped, not DLQ-routed")
                .register(registry);
        persistFailures = Counter.builder("chaosforge.run_projection.persist_failures")
                .description("Individual run_projection upsert attempts that failed transiently; "
                        + "the container error handler retries with backoff")
                .register(registry);
        persistFailuresExhausted = Counter.builder("chaosforge.run_projection.persist_failures_exhausted")
                .description("run_projection upserts that failed every retry and were permanently "
                        + "given up on (not DLQ-routed)")
                .register(registry);
    }

    public void decodeFailure() {
        decodeFailures.increment();
    }

    public void persistFailure() {
        persistFailures.increment();
    }

    public void persistFailureExhausted() {
        persistFailuresExhausted.increment();
    }
}
