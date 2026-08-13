package io.chaosforge.execution.config;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.chaosforge.execution.dlq.DlqRoutableException;
import io.chaosforge.execution.observability.ExecutionMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Command consumer: String key + byte[] (Avro) value, manual ack, no auto-commit. Failures route
 * straight to {@code <topic>.DLQ} with an {@code x-dlq-reason} header — no in-container retry (the
 * separate DLQ retry consumer republishes the replayable reasons). A poison pill therefore advances
 * the partition rather than stalling it.
 */
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, byte[]> consumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.consumer.group-id}") String groupId,
            MeterRegistry meterRegistry) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cfg.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);   // 5m; step aggregate timeout bounded below
        DefaultKafkaConsumerFactory<String, byte[]> factory = new DefaultKafkaConsumerFactory<>(cfg);
        // Binds Kafka client metrics → kafka.consumer.lag etc. (architecture specifications §SLIs: command consumer lag).
        factory.addListener(new MicrometerConsumerListener<>(meterRegistry));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]> kafkaListenerContainerFactory(
            ConsumerFactory<String, byte[]> consumerFactory, KafkaTemplate<String, byte[]> execKafkaTemplate,
            ExecutionMetrics metrics,
            @Value("${chaosforge.kafka.command-concurrency:1}") int concurrency) {
        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        // Safe to raise up to partition count: ordering/fencing come from inbox_fence, not the thread.
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(AckMode.MANUAL);
        // Joins the W3C traceparent from the CP relay's producer so pipeline logs share a trace_id.
        factory.getContainerProperties().setObservationEnabled(true);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                deadLetterRecoverer(execKafkaTemplate, metrics),
                new FixedBackOff(0L, 0L));   // recover immediately to DLQ, no retry budget
        // Manual ack doesn't durably commit a recovered record's offset without this — a poison
        // record at the partition tail could be redelivered after a rebalance (Spring Kafka reference).
        errorHandler.setCommitRecovered(true);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    /**
     * DLQ retry consumer factory (dlq-rules.md, ADR-0529). No DeadLetterPublishingRecoverer — a
     * failed republish retries with fixed backoff instead of chaining to {@code .DLQ.DLQ}, except
     * {@link RecordTooLargeException} (arch-audit F-08), which can never self-heal and would
     * otherwise freeze the lane forever. AuthorizationException stays retryable (can be transient).
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]> dlqRetryListenerContainerFactory(
            ConsumerFactory<String, byte[]> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(AckMode.MANUAL);
        // Same trace_id/MDC correlation on the repair lane's log lines.
        factory.getContainerProperties().setObservationEnabled(true);
        DefaultErrorHandler repairHandler = new DefaultErrorHandler(new FixedBackOff(2_000L, Long.MAX_VALUE));
        repairHandler.addNotRetryableExceptions(RecordTooLargeException.class);
        factory.setCommonErrorHandler(repairHandler);
        return factory;
    }

    private DeadLetterPublishingRecoverer deadLetterRecoverer(KafkaTemplate<String, byte[]> template,
                                                              ExecutionMetrics metrics) {
        // Same-index resolve: command partition p → .DLQ partition p (DLQ has >= as many partitions).
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new TopicPartition(record.topic() + ".DLQ", record.partition()));
        recoverer.setHeadersFunction((record, ex) -> {
            String reason = dlqReasonFor(ex);
            Headers headers = new RecordHeaders(record.headers().toArray());
            headers.add("x-dlq-reason", reason.getBytes(UTF_8));   // exactly one value
            metrics.dlqRouted(record.topic() + ".DLQ", reason);    // single DLQ-routing point
            return headers;
        });
        return recoverer;
    }

    /** Map the thrown exception to its reason; an unmapped failure fails closed to SCHEMA_INVALID. */
    private static String dlqReasonFor(Exception ex) {
        Throwable cause = (ex instanceof ListenerExecutionFailedException && ex.getCause() != null)
                ? ex.getCause() : ex;
        if (cause instanceof DlqRoutableException routable) {
            return routable.dlqReason();
        }
        return "SCHEMA_INVALID";   // never guess a replayable class (dlq-rules.md)
    }
}
