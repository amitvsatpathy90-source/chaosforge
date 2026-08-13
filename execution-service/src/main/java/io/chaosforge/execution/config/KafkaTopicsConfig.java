package io.chaosforge.execution.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Explicit topic provisioning (arch-audit LOW-4) — deterministic partition counts instead of
 * relying on broker auto-create. KafkaAdmin reconciles at startup, only ever increasing partitions.
 */
@Configuration
public class KafkaTopicsConfig {

    /**
     * GAP-04: DLQ partitions must be >= command partitions — the recoverer resolves same-index,
     * and a smaller DLQ silently breaks that for high-index partitions. Fail fast at startup.
     */
    public KafkaTopicsConfig(
            @Value("${chaosforge.kafka.command-partitions:12}") int commandPartitions,
            @Value("${chaosforge.kafka.dlq-partitions:12}") int dlqPartitions) {
        if (dlqPartitions < commandPartitions) {
            throw new IllegalStateException(String.format(
                    "chaosforge.kafka.dlq-partitions (%d) must be >= chaosforge.kafka.command-partitions (%d) "
                    + "so the DLQ recoverer's same-index resolve is always valid (GAP-04)",
                    dlqPartitions, commandPartitions));
        }
    }

    @Bean
    public NewTopic commandTopic(
            @Value("${chaosforge.kafka.command-topic}") String topic,
            @Value("${chaosforge.kafka.command-partitions:12}") int partitions,
            @Value("${chaosforge.kafka.replication-factor:1}") short replicationFactor) {
        return TopicBuilder.name(topic).partitions(partitions).replicas(replicationFactor).build();
    }

    // DLQ partition count must be >= command topic's (arch-audit L1) for same-index resolve to hold.
    @Bean
    public NewTopic commandDlqTopic(
            @Value("${chaosforge.kafka.command-topic}") String topic,
            @Value("${chaosforge.kafka.dlq-partitions:12}") int partitions,
            @Value("${chaosforge.kafka.replication-factor:1}") short replicationFactor) {
        return TopicBuilder.name(topic + ".DLQ").partitions(partitions).replicas(replicationFactor).build();
    }

    @Bean
    public NewTopic resultsTopic(
            @Value("${chaosforge.kafka.results-topic:chaosforge.scenario.results.v1}") String topic,
            @Value("${chaosforge.kafka.results-partitions:12}") int partitions,
            @Value("${chaosforge.kafka.replication-factor:1}") short replicationFactor) {
        return TopicBuilder.name(topic).partitions(partitions).replicas(replicationFactor).build();
    }
}
