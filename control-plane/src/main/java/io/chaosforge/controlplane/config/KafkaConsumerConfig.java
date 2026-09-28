package io.chaosforge.controlplane.config;

import java.util.HashMap;
import java.util.Map;

import io.chaosforge.controlplane.pipeline.RunProjectionMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * run_projection is a rebuildable read cache, not a correctness-critical inbox — never DLQ-routed
 * (locked decision, Batch 4). Decode failures are skipped inside the listener; transient persist
 * failures are retried in place by the error handler below, then given up on and counted.
 */
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, byte[]> runProjectionConsumerFactory(
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
        DefaultKafkaConsumerFactory<String, byte[]> factory = new DefaultKafkaConsumerFactory<>(cfg);
        factory.addListener(new MicrometerConsumerListener<>(meterRegistry));   // kafka.consumer.lag
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]> runProjectionListenerContainerFactory(
            ConsumerFactory<String, byte[]> runProjectionConsumerFactory,
            RunProjectionMetrics metrics) {
        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(runProjectionConsumerFactory);
        factory.getContainerProperties().setAckMode(AckMode.MANUAL);
        factory.getContainerProperties().setObservationEnabled(true);   // shares trace_id with the exec-side publish
        // 1 attempt + 2 retries, 2s apart, on this same consumer — no restart or rebalance needed.
        // On exhaustion: count, then the container skips past the record (no DLQ, no wedged partition).
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (record, exception) -> metrics.persistFailureExhausted(),
                new FixedBackOff(2000L, 2L));
        errorHandler.setCommitRecovered(true);   // MANUAL ack: commit the skipped record's offset
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
