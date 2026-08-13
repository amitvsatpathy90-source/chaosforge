package io.chaosforge.execution.observability;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * Execution-service SLIs (execution-service-rules.md §Observability, architecture specifications §SLIs). Counters only;
 * <b>never</b> a {@code tenant_id} tag (cardinality bomb — tenant data lives on traces/logs). The
 * dimensions used here ({@code outcome}, {@code dlq_reason}, {@code topic}) are all low-cardinality.
 */
@Component
public class ExecutionMetrics {

    private static final String EXECUTION = "chaosforge.scenario.execution";
    /**
     * Counts DLQ <b>routings</b>, not depth (arch-audit F5). It is monotonic: draining or replaying a
     * record never decrements it, so it can only answer "how many records have we ever routed", never
     * "how many are sitting there now". It was previously named {@code chaosforge.dlq.depth}, which
     * promised the latter — {@code alerts.yml} always read it correctly as a {@code rate()}, but the
     * architecture specifications SLO "DLT depth = 0" was unachievable by construction against a counter that never
     * returns to 0. There is deliberately <b>no</b> standing-backlog signal: Kafka retains records after
     * ack, and the retry consumer acks hard poison it will never republish (dlq-rules.md), so
     * consumer-group lag reaches 0 with poison still in the topic. A true depth gauge needs a
     * human-triage watermark — a design decision, not a metric tweak. Do not rename this back to
     * "depth" without building that.
     */
    private static final String DLQ_ROUTED = "chaosforge.dlq.routed";
    private static final String DLQ_RETRY = "chaosforge.dlq.retry";
    private static final String INBOX_DEDUP = "inbox.duplicates_suppressed_total";
    private static final String SWEPT_INCOMPLETE = "chaosforge.run.swept_incomplete_total";

    private final MeterRegistry registry;

    public ExecutionMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    /** A command reached a terminal result and was finalized. */
    public void success() {
        registry.counter(EXECUTION, "outcome", "success", "dlq_reason", "none").increment();
    }

    /** Inbox/​fence saw this message before — suppressed as a duplicate (no business effect re-run). */
    public void deduped() {
        registry.counter(EXECUTION, "outcome", "deduped", "dlq_reason", "none").increment();
        registry.counter(INBOX_DEDUP).increment();
    }

    /** A command was routed to the DLQ. Called from the single DLQ-routing point (recoverer). */
    public void dlqRouted(String dlqTopic, String dlqReason) {
        registry.counter(EXECUTION, "outcome", "dlq-routed", "dlq_reason", dlqReason).increment();
        registry.counter(DLQ_ROUTED, "topic", dlqTopic, "dlq_reason", dlqReason).increment();
    }

    /**
     * Runs left {@code IN_PROGRESS} by a Phase-2→3 crash, marked {@code INCOMPLETE} by the sweep
     * (Defect A). A non-zero rate means executions are crashing before finalize — alert surface.
     */
    public void sweptIncomplete(int count) {
        registry.counter(SWEPT_INCOMPLETE).increment(count);
    }

    /**
     * DLQ retry-consumer disposition (ADR-0529). {@code outcome} = republished | skipped | exhausted;
     * {@code dlq_reason} is the originating reason. Both are low-cardinality. A rising {@code exhausted}
     * rate means a target is persistently down past the attempt budget — alert surface.
     */
    public void dlqRetry(String outcome, String dlqReason) {
        registry.counter(DLQ_RETRY, "outcome", outcome, "dlq_reason", dlqReason == null ? "none" : dlqReason)
                .increment();
    }
}
