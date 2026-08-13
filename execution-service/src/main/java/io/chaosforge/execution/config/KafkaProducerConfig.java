package io.chaosforge.execution.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Producer used by the DLQ {@code DeadLetterPublishingRecoverer} (and, later, the result-event
 * outbox relay). String key + byte[] value, mirroring the command wire shape.
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, byte[]> execProducerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        cfg.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        cfg.put(ProducerConfig.ACKS_CONFIG, "all");
        // ADR-0528: bound in-flight + delivery time so a stuck broker fails deterministically; row
        // stays PENDING and retries from durable state.
        cfg.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        cfg.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        // Fail fast on a full buffer instead of blocking up to max.block.ms (arch-audit A-2).
        cfg.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2_000);
        return new DefaultKafkaProducerFactory<>(cfg);
    }

    @Bean
    public KafkaTemplate<String, byte[]> execKafkaTemplate(ProducerFactory<String, byte[]> execProducerFactory) {
        KafkaTemplate<String, byte[]> template = new KafkaTemplate<>(execProducerFactory);
        // Injects W3C traceparent headers on relay/DLQ/retry sends so downstream logs share a trace_id.
        template.setObservationEnabled(true);
        return template;
    }
}
