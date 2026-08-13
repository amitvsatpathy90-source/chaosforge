package io.chaosforge.execution.dlq;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.github.f4b6a3.uuid.UuidCreator;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Rebuilds and sends DLQ records (dlq-rules.md, ADR-0529). A replayable record republishes to the
 * main topic with a fresh {@code x-message-id} — reusing the original would be ack-skipped by inbox
 * dedup (claim commits before step execution) and never re-execute. Step-level idempotency
 * ({@code scenario:replay_version:step_id}) re-runs only the failed step; same replay_version still
 * passes the fence. Original Avro value + partition key preserved so the consumer decodes unchanged.
 */
@Component
public class DlqRepublisher {

    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);
    private static final Set<String> STRIP_ON_RETRY = Set.of("x-message-id", "x-dlq-reason", "x-dlq-attempt");
    private static final Set<String> STRIP_ON_PARK = Set.of("x-dlq-reason");

    private final KafkaTemplate<String, byte[]> kafka;
    private final String mainTopic;
    private final String dlqTopic;

    public DlqRepublisher(KafkaTemplate<String, byte[]> execKafkaTemplate,
                          @Value("${chaosforge.kafka.command-topic}") String mainTopic) {
        this.kafka = execKafkaTemplate;
        this.mainTopic = mainTopic;
        this.dlqTopic = mainTopic + ".DLQ";
    }

    /** Republish to the main topic with a fresh message_id and the incremented attempt counter. */
    public void republishToMain(ConsumerRecord<String, byte[]> dlqRecord, int nextAttempt) {
        ProducerRecord<String, byte[]> out =
                new ProducerRecord<>(mainTopic, null, dlqRecord.key(), dlqRecord.value());
        copyHeadersExcept(dlqRecord, out, STRIP_ON_RETRY);
        out.headers().add("x-message-id", UuidCreator.getTimeOrderedEpoch().toString().getBytes(UTF_8));
        out.headers().add("x-dlq-attempt", Integer.toString(nextAttempt).getBytes(UTF_8));
        send(out);
    }

    /** Attempt budget spent — park back in the DLQ as terminal {@code RETRY_EXHAUSTED} for human review. */
    public void parkExhausted(ConsumerRecord<String, byte[]> dlqRecord) {
        ProducerRecord<String, byte[]> out =
                new ProducerRecord<>(dlqTopic, null, dlqRecord.key(), dlqRecord.value());
        copyHeadersExcept(dlqRecord, out, STRIP_ON_PARK);   // keep message_id + attempt for forensics
        out.headers().add("x-dlq-reason", DlqRetryPolicy.RETRY_EXHAUSTED.getBytes(UTF_8));
        send(out);
    }

    private static void copyHeadersExcept(ConsumerRecord<String, byte[]> from,
                                          ProducerRecord<String, byte[]> to, Set<String> strip) {
        for (Header h : from.headers()) {
            if (!strip.contains(h.key())) {
                to.headers().add(h.key(), h.value());
            }
        }
    }

    private void send(ProducerRecord<String, byte[]> record) {
        try {
            kafka.send(record).get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DlqRepublishException("interrupted publishing to " + record.topic(), e);
        } catch (ExecutionException | TimeoutException e) {
            // Broker rejected or no ack within SEND_TIMEOUT — surface so the consumer does NOT ack.
            throw new DlqRepublishException("failed publishing to " + record.topic(), e);
        }
    }
}
