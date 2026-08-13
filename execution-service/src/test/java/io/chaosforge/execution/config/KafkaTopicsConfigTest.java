package io.chaosforge.execution.config;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * GAP-04: the DLQ partition count must be {@code >=} the command topic's, or the
 * {@code DeadLetterPublishingRecoverer}'s same-index resolve silently breaks for high-index partitions.
 * The check is at construction so a drifted pair fails startup, not in production.
 */
class KafkaTopicsConfigTest {

    @Test
    void dlqFewerPartitionsThanCommand_failsFast() {
        assertThatThrownBy(() -> new KafkaTopicsConfig(12, 6))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("dlq-partitions")
                .hasMessageContaining("command-partitions");
    }

    @Test
    void dlqEqualOrMorePartitions_ok() {
        assertThatCode(() -> new KafkaTopicsConfig(12, 12)).doesNotThrowAnyException();
        assertThatCode(() -> new KafkaTopicsConfig(6, 12)).doesNotThrowAnyException();
    }
}
