package io.chaosforge.controlplane.ai;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

/**
 * Read-only, point lookup of one DLQ record's <b>headers</b> for advisory triage (ai-rules.md
 * Tier 2). Structural guarantees, all load-bearing:
 *
 * <ul>
 *   <li><b>Read-only:</b> the throwaway consumer has no {@code group.id} — it structurally cannot
 *       commit an offset — and nothing here produces, acks, or republishes.</li>
 *   <li><b>DLQ-only:</b> refuses any topic not ending {@code .DLQ}; without this the endpoint
 *       would be a generic Kafka reader (an operator could dump command-topic payloads).</li>
 *   <li><b>Redaction by construction (ADR-0519):</b> only the {@link DlqEnvelope} fields are ever
 *       extracted — payload bytes and tenant/scenario headers are never read out of the record.</li>
 * </ul>
 */
@Component
public class DlqRecordReader {

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(3);
    private static final int SUMMARY_MAX_CHARS = 300;

    private final String bootstrapServers;

    public DlqRecordReader(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /** @throws IllegalArgumentException (→ 400) for a non-DLQ topic or negative coordinates */
    public Optional<DlqEnvelope> read(String topic, int partition, long offset) {
        if (!topic.endsWith(".DLQ")) {
            throw new IllegalArgumentException("only .DLQ topics are readable by triage");
        }
        if (partition < 0 || offset < 0) {
            throw new IllegalArgumentException("partition and offset must be non-negative");
        }
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);   // belt-and-suspenders; no group.id anyway

        TopicPartition tp = new TopicPartition(topic, partition);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(cfg)) {
            consumer.assign(List.of(tp));
            long beginning = consumer.beginningOffsets(List.of(tp)).getOrDefault(tp, 0L);
            long end = consumer.endOffsets(List.of(tp)).getOrDefault(tp, 0L);
            if (offset < beginning || offset >= end) {
                return Optional.empty();   // outside the retained range → not found
            }
            consumer.seek(tp, offset);
            long deadline = System.currentTimeMillis() + POLL_TIMEOUT.toMillis();
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(250));
                for (ConsumerRecord<byte[], byte[]> record : records.records(tp)) {
                    // Exact-offset match only — a later offset means the requested record is gone.
                    return record.offset() == offset ? Optional.of(toEnvelope(record)) : Optional.empty();
                }
            }
            return Optional.empty();
        }
    }

    private static DlqEnvelope toEnvelope(ConsumerRecord<byte[], byte[]> record) {
        return new DlqEnvelope(
                headerString(record, "x-dlq-reason", "unknown"),
                firstLine(headerString(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE, "")),
                headerString(record, KafkaHeaders.DLT_ORIGINAL_TOPIC, "unknown"),
                headerInt(record, "x-dlq-attempt"));
    }

    /** First line only, length-capped — never a stack trace, never payload content (ADR-0519). */
    private static String firstLine(String value) {
        String line = value.lines().findFirst().orElse("").strip();
        return line.length() <= SUMMARY_MAX_CHARS ? line : line.substring(0, SUMMARY_MAX_CHARS);
    }

    private static String headerString(ConsumerRecord<byte[], byte[]> record, String key, String fallback) {
        Header h = record.headers().lastHeader(key);
        return (h == null || h.value() == null) ? fallback : new String(h.value(), UTF_8);
    }

    private static int headerInt(ConsumerRecord<byte[], byte[]> record, String key) {
        try {
            return Integer.parseInt(headerString(record, key, "0"));
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
