package io.chaosforge.controlplane.dlq;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Publishes the standing DLQ-depth signal arch-audit F5 found missing (ADR-0542):
 * {@code untriaged_depth = max(0, endOffset - max(reviewedOffset, beginningOffset))} per
 * (topic, partition) — records past the human-triage watermark. Neither the routing counter nor
 * consumer-group lag can express this (lag hits 0 with poison present — ADR-0529).
 * Read-only consumer: no group.id, never consumes, no auto-create. Grain is (topic, partition) not
 * dlq_reason — a partition interleaves reasons, splitting needs an O(depth) scan. No tenant_id label.
 * Fail-soft: a broker error on one topic keeps its last-known value, logs WARN, and increments
 * {@code chaosforge.dlq.depth.poll_failures} — without that counter a frozen gauge (0 at boot) is
 * indistinguishable from an empty DLQ (the false-healthy mode RPE's Bundle-1 audit fixed; mirrored
 * here). Backs the DlqDepthPollFailing meta-alert.
 */
@Component
public class DlqDepthGauge {

    private static final Logger log = LoggerFactory.getLogger(DlqDepthGauge.class);
    private static final String METRIC = "chaosforge.dlq.untriaged_depth";

    private final String bootstrapServers;
    private final List<String> dlqTopics;
    private final Duration requestTimeout;
    private final DlqTriageWatermarkDao watermarks;
    private final MeterRegistry meterRegistry;

    // One AtomicLong per (topic, partition); registered lazily on first sighting, thread-safe.
    private final Map<TopicPartition, AtomicLong> depths = new ConcurrentHashMap<>();

    // One failure counter per configured topic, registered at construction — the series must exist
    // from boot so increase() has a 0 baseline before the first failure.
    private final Map<String, Counter> pollFailures = new HashMap<>();

    public DlqDepthGauge(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${chaosforge.dlq.depth.topics:chaosforge.scenario.commands.v1.DLQ}") List<String> dlqTopics,
            @Value("${chaosforge.dlq.depth.request-timeout-ms:5000}") long requestTimeoutMs,
            DlqTriageWatermarkDao watermarks,
            MeterRegistry meterRegistry) {
        this.bootstrapServers = bootstrapServers;
        this.dlqTopics = dlqTopics;
        this.requestTimeout = Duration.ofMillis(requestTimeoutMs);
        this.watermarks = watermarks;
        this.meterRegistry = meterRegistry;
        for (String topic : dlqTopics) {
            pollFailures.put(topic, Counter.builder("chaosforge.dlq.depth.poll_failures")
                    .tag("topic", topic)
                    .description("Failed depth polls; while rising, untriaged_depth for this topic is stale")
                    .register(meterRegistry));
        }
    }

    /** Best-effort initial reading; a DLQ topic that doesn't exist yet is simply skipped until it does. */
    @EventListener(ApplicationReadyEvent.class)
    public void onStartup() {
        refresh();
    }

    @Scheduled(fixedDelayString = "${chaosforge.dlq.depth.refresh-ms:30000}",
            initialDelayString = "${chaosforge.dlq.depth.refresh-ms:30000}")
    public void scheduledRefresh() {
        refresh();
    }

    /** Recompute depth for every partition of every configured DLQ topic and store it into the gauges. */
    public void refresh() {
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfig())) {
            for (String topic : dlqTopics) {
                try {
                    refreshTopic(consumer, topic);
                } catch (RuntimeException e) {
                    pollFailures.get(topic).increment();
                    log.warn("DLQ depth refresh failed for {}; keeping last-known values", topic, e);
                }
            }
        } catch (RuntimeException e) {
            // Consumer could not even open — every configured topic's gauge is stale; count them all.
            pollFailures.values().forEach(Counter::increment);
            log.warn("DLQ depth refresh could not open a consumer; keeping last-known values", e);
        }
    }

    private void refreshTopic(KafkaConsumer<byte[], byte[]> consumer, String topic) {
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic, requestTimeout);
        if (partitionInfos == null || partitionInfos.isEmpty()) {
            return;   // topic not created yet (or no metadata) — nothing to measure
        }
        List<TopicPartition> tps = new ArrayList<>(partitionInfos.size());
        for (PartitionInfo pi : partitionInfos) {
            tps.add(new TopicPartition(topic, pi.partition()));
        }
        Map<TopicPartition, Long> beginnings = consumer.beginningOffsets(tps, requestTimeout);
        Map<TopicPartition, Long> ends = consumer.endOffsets(tps, requestTimeout);
        for (TopicPartition tp : tps) {
            long begin = beginnings.getOrDefault(tp, 0L);
            long end = ends.getOrDefault(tp, 0L);
            // No watermark → full-topic depth. Clamp with begin so a stranded watermark can't overcount.
            long reviewed = Math.max(watermarks.reviewedOffset(tp.topic(), tp.partition()).orElse(begin), begin);
            long depth = Math.max(0L, end - reviewed);
            gaugeFor(tp).set(depth);
        }
    }

    private AtomicLong gaugeFor(TopicPartition tp) {
        return depths.computeIfAbsent(tp, key -> {
            AtomicLong holder = new AtomicLong(0L);
            Gauge.builder(METRIC, holder, AtomicLong::doubleValue)
                    .tag("topic", key.topic())
                    .tag("partition", Integer.toString(key.partition()))
                    .description("DLQ records past the human-triage watermark, awaiting review (ADR-0542)")
                    .register(meterRegistry);
            return holder;
        });
    }

    private Map<String, Object> consumerConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);        // no group.id anyway — cannot commit
        cfg.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);  // the gauge must never create a DLQ topic
        return cfg;
    }
}
