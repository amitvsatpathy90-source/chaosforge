package io.chaosforge.execution.dlq;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.chaosforge.execution.observability.ExecutionMetrics;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * DLQ retry consumer (dlq-rules.md, ADR-0529) — a <b>separate</b> consumer group, the only component
 * permitted to republish to the main topic. Reads {@code <command-topic>.DLQ}; republishes only the
 * replayable reasons ({@code INFRA_TRANSIENT}, {@code STEP_TIMEOUT}) with exponential backoff + full
 * jitter; parks exhausted records as {@code RETRY_EXHAUSTED}; leaves hard-poison records untouched for
 * human triage. Returns {@code void}. Acks only after the decided action succeeds; a failed republish
 * throws (no ack) → the record is redelivered with backoff by the container error handler.
 */
@Component
public class DlqRetryConsumer {

    private static final Logger log = LoggerFactory.getLogger(DlqRetryConsumer.class);

    private final DlqRetryPolicy policy;
    private final DlqRepublisher republisher;
    private final ExecutionMetrics metrics;

    public DlqRetryConsumer(DlqRetryPolicy policy, DlqRepublisher republisher, ExecutionMetrics metrics) {
        this.policy = policy;
        this.republisher = republisher;
        this.metrics = metrics;
    }

    @KafkaListener(topics = "${chaosforge.kafka.command-topic}.DLQ",
            groupId = "${chaosforge.dlq.retry.group-id:dlq-retry}",
            containerFactory = "dlqRetryListenerContainerFactory")
    public void onDlqRecord(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        String reason = header(record, "x-dlq-reason");
        int attempt = intHeader(record, "x-dlq-attempt");

        switch (policy.decide(reason, attempt)) {
            case RetryDecision.Skip skip -> {
                // Hard poison / unknown — never republished; just advance our offset past it.
                metrics.dlqRetry("skipped", skip.reason());
            }
            case RetryDecision.Republish r -> {
                sleepWithJitter(r.backoffMillis());
                republisher.republishToMain(record, r.nextAttempt());   // throws → no ack → redelivery
                metrics.dlqRetry("republished", reason);
                log.info("DLQ republished to main: reason={} attempt={} partition={} offset={}",
                        reason, r.nextAttempt(), record.partition(), record.offset());
            }
            case RetryDecision.Exhausted e -> {
                republisher.parkExhausted(record);                      // throws → no ack → redelivery
                metrics.dlqRetry("exhausted", reason);
                log.warn("DLQ retry exhausted after {} attempts: reason={} partition={} offset={}",
                        e.attempts(), reason, record.partition(), record.offset());
            }
        }
        ack.acknowledge();   // post-action only
    }

    private static String header(ConsumerRecord<?, ?> record, String key) {
        Header h = record.headers().lastHeader(key);
        return h == null ? null : new String(h.value(), UTF_8);
    }

    private static int intHeader(ConsumerRecord<?, ?> record, String key) {
        String value = header(record, key);
        if (value == null) {
            return 0;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return 0;   // malformed counter → treat as first attempt; the policy still bounds attempts
        }
    }

    /**
     * Full jitter (AWS "Exponential Backoff and Jitter"): sleep a uniform-random duration in
     * {@code [0, ceiling]}. The ceiling is bounded below {@code max.poll.interval.ms} so the repair
     * lane never sleeps long enough to trigger a rebalance.
     */
    private static void sleepWithJitter(long ceilingMillis) {
        long sleep = ceilingMillis <= 0 ? 0 : ThreadLocalRandom.current().nextLong(ceilingMillis + 1);
        if (sleep > 0) {
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DlqRepublishException("interrupted during retry backoff", e);
            }
        }
    }
}
