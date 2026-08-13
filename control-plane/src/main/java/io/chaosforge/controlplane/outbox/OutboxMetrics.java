package io.chaosforge.controlplane.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Outbox SLIs (architecture §11). Gauges only — NO tenant_id label (cardinality bomb).
 */
@Component
public class OutboxMetrics {

    public OutboxMetrics(MeterRegistry registry, OutboxRelayDao dao,
                         @Value("${chaosforge.outbox.claim-window-days:2}") int claimWindowDays) {
        Gauge.builder("outbox.pending_count", dao, OutboxRelayDao::pendingCount)
                .description("Outbox rows awaiting publish")
                .register(registry);
        Gauge.builder("outbox.oldest_pending_age_seconds", dao, OutboxRelayDao::oldestPendingAgeSeconds)
                .description("Age of the oldest PENDING outbox row")
                .baseUnit("seconds")
                .register(registry);
        Gauge.builder("outbox.dead_count", dao, OutboxRelayDao::deadCount)
                .description("Outbox rows quarantined as DEAD after max record-fatal publish attempts (ADR-0528)")
                .register(registry);
        // Straggler-lane-only rows; should be ~0. Sustained non-zero risks aging past partition drop.
        Gauge.builder("outbox.pending_outside_claim_window", dao,
                        d -> d.pendingOutsideWindowCount(claimWindowDays))
                .description("PENDING outbox rows older than the hot-lane claim window (straggler-lane only)")
                .register(registry);
    }
}
