package io.chaosforge.execution.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Execution Service result-outbox SLIs (architecture specifications §SLIs). Gauges only — <b>never</b> a
 * {@code tenant_id} label (cardinality bomb). A stalled relay surfaces as a rising
 * {@code outbox.oldest_pending_age_seconds}; a poison result event surfaces as {@code outbox.dead_count}.
 */
@Component
public class ExecOutboxMetrics {

    public ExecOutboxMetrics(MeterRegistry registry, ExecOutboxRelayDao dao,
                             @Value("${chaosforge.outbox.claim-window-days:2}") int claimWindowDays) {
        Gauge.builder("outbox.pending_count", dao, ExecOutboxRelayDao::pendingCount)
                .description("Result-event outbox rows awaiting publish")
                .register(registry);
        Gauge.builder("outbox.oldest_pending_age_seconds", dao, ExecOutboxRelayDao::oldestPendingAgeSeconds)
                .description("Age of the oldest PENDING result-event outbox row")
                .baseUnit("seconds")
                .register(registry);
        Gauge.builder("outbox.dead_count", dao, ExecOutboxRelayDao::deadCount)
                .description("Result-event outbox rows quarantined as DEAD after max record-fatal publish attempts (ADR-0528)")
                .register(registry);
        // Straggler-lane-only rows; should be ~0. Sustained non-zero risks aging past partition drop.
        Gauge.builder("outbox.pending_outside_claim_window", dao,
                        d -> d.pendingOutsideWindowCount(claimWindowDays))
                .description("PENDING result-event rows older than the hot-lane claim window (straggler-lane only)")
                .register(registry);
    }
}
