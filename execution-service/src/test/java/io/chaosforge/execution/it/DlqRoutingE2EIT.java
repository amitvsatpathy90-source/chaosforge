package io.chaosforge.execution.it;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.chaosforge.execution.config.KafkaConsumerConfig;
import io.chaosforge.execution.dlq.DlqRepublisher;
import io.chaosforge.execution.dlq.DlqRetryConsumer;
import io.chaosforge.execution.dlq.DlqRetryPolicy;
import io.chaosforge.execution.observability.ExecutionMetrics;
import io.chaosforge.execution.pipeline.ClaimPhase;
import io.chaosforge.execution.pipeline.CommandDecoder;
import io.chaosforge.execution.pipeline.ExecutePhase;
import io.chaosforge.execution.pipeline.FinalizePhase;
import io.chaosforge.execution.pipeline.ScenarioCommandListener;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end DLQ routing + retry-consumer round-trip against a <b>real</b> broker (Testcontainers
 * Apache Kafka — the test analog of the Redpanda runtime broker). The <b>real production beans</b> are
 * assembled by hand and bound to live listener containers: the {@code DeadLetterPublishingRecoverer}
 * and {@code DefaultErrorHandler} from {@link KafkaConsumerConfig}, the real {@link CommandDecoder} on
 * the main path, and the real {@link DlqRetryConsumer}/{@link DlqRepublisher}/{@link DlqRetryPolicy} on
 * the retry path. No Spring context / Postgres / Flyway — those are exercised by the other ITs and
 * only add flakiness to a Kafka-routing test.
 *
 * <p>Isolation: a unique partition key per test + verification consumers that manually assign and
 * seek-to-beginning (deterministic, no rebalance-join latency). Backoff is 0 so the replayable hop is
 * near-instant.
 */
@Testcontainers
class DlqRoutingE2EIT {

    static final String COMMANDS = "chaosforge.scenario.commands.v1";
    static final String COMMANDS_DLQ = COMMANDS + ".DLQ";

    private static final Duration AWAIT = Duration.ofSeconds(15);

    @Container
    @SuppressWarnings("resource")
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

    private static KafkaTemplate<String, byte[]> template;
    private static final List<ConcurrentMessageListenerContainer<String, byte[]>> CONTAINERS = new ArrayList<>();

    @BeforeAll
    static void wireRealBeansAgainstRealBroker() throws Exception {
        createTopics();

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        template = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ExecutionMetrics metrics = new ExecutionMetrics(registry);
        KafkaConsumerConfig kafkaConfig = new KafkaConsumerConfig();

        // --- Main command listener: real CommandDecoder + the production DLQ recoverer/error handler.
        // The claim/execute/finalize phases are never reached on the poison path (decode throws first).
        ScenarioCommandListener mainListener = new ScenarioCommandListener(
                new CommandDecoder(7, 4), mock(ClaimPhase.class), mock(ExecutePhase.class),
                mock(FinalizePhase.class), metrics);
        ConsumerFactory<String, byte[]> mainCf =
                kafkaConfig.consumerFactory(KAFKA.getBootstrapServers(), "execution-service", registry);
        ConcurrentKafkaListenerContainerFactory<String, byte[]> mainFactory =
                kafkaConfig.kafkaListenerContainerFactory(mainCf, template, metrics, 1);
        startContainer(mainFactory, COMMANDS, "execution-service", mainListener::onCommand);

        // --- DLQ retry consumer: real policy + republisher, backoff disabled for test speed.
        DlqRetryConsumer retryConsumer = new DlqRetryConsumer(
                new DlqRetryPolicy(5, 0L, 0L), new DlqRepublisher(template, COMMANDS), metrics);
        ConsumerFactory<String, byte[]> retryCf =
                kafkaConfig.consumerFactory(KAFKA.getBootstrapServers(), "dlq-retry", registry);
        ConcurrentKafkaListenerContainerFactory<String, byte[]> retryFactory =
                kafkaConfig.dlqRetryListenerContainerFactory(retryCf);
        startContainer(retryFactory, COMMANDS_DLQ, "dlq-retry", retryConsumer::onDlqRecord);
    }

    @AfterAll
    static void stop() {
        CONTAINERS.forEach(ConcurrentMessageListenerContainer::stop);
        if (template != null) {
            template.destroy();
        }
    }

    @Test
    void poisonCommand_isRoutedToDlq_withSchemaInvalid() {
        // A non-Avro payload fails at pipeline step 1 (decode); the real recoverer must land it on
        // <topic>.DLQ tagged SCHEMA_INVALID, payload preserved for triage.
        String key = "route-" + UUID.randomUUID();
        publish(COMMANDS, key, "not-valid-avro".getBytes(UTF_8), Map.of());

        try (Consumer<String, byte[]> dlq = verificationConsumer(COMMANDS_DLQ)) {
            ConsumerRecord<String, byte[]> routed = awaitRecord(dlq, r -> key.equals(r.key()), AWAIT);
            assertThat(routed).as("poison command must reach the DLQ").isNotNull();
            assertThat(header(routed, "x-dlq-reason")).isEqualTo("SCHEMA_INVALID");
            assertThat(routed.value()).isEqualTo("not-valid-avro".getBytes(UTF_8));
        }
    }

    @Test
    void consecutivePoisonRecords_bothReachDlq_partitionNeverStalls() {
        // The canonical poison-pill hazard (C14): a record that always fails decode must not block the
        // partition. The production DefaultErrorHandler routes each poison to the DLQ and commits past
        // it, so a SECOND poison published behind the first is still processed. If the partition stalled
        // on the first, the second would never reach the DLQ — so observing BOTH proves advance-past.
        String first = "poison1-" + UUID.randomUUID();
        String second = "poison2-" + UUID.randomUUID();
        publish(COMMANDS, first, "still-not-avro".getBytes(UTF_8), Map.of());
        publish(COMMANDS, second, "also-not-avro".getBytes(UTF_8), Map.of());

        try (Consumer<String, byte[]> dlq = verificationConsumer(COMMANDS_DLQ)) {
            Map<String, ConsumerRecord<String, byte[]>> routed =
                    awaitKeys(dlq, Set.of(first, second), AWAIT);
            assertThat(routed)
                    .as("both consecutive poison records reach the DLQ — the partition advanced past the first")
                    .containsKeys(first, second);
            assertThat(header(routed.get(second), "x-dlq-reason")).isEqualTo("SCHEMA_INVALID");
        }
    }

    @Test
    void replayableReason_isRepublishedToMain_withFreshMessageId() {
        // A replayable DLQ record must be republished to the MAIN topic with a FRESH message_id (so the
        // execution inbox does not dedup the retry away — ADR-0529).
        String key = "replay-" + UUID.randomUUID();
        String originalMessageId = UUID.randomUUID().toString();
        Map<String, String> headers = new HashMap<>();
        headers.put("x-dlq-reason", "INFRA_TRANSIENT");
        headers.put("x-dlq-attempt", "0");
        headers.put("x-message-id", originalMessageId);
        headers.put("x-replay-version", "5");
        headers.put("x-rule-set-id", UUID.randomUUID().toString());
        headers.put("x-rule-set-version", "3");
        headers.put("x-tenant-id", UUID.randomUUID().toString());
        publish(COMMANDS_DLQ, key, "command-bytes".getBytes(UTF_8), headers);

        try (Consumer<String, byte[]> main = verificationConsumer(COMMANDS)) {
            ConsumerRecord<String, byte[]> republished = awaitRecord(main,
                    r -> key.equals(r.key()) && "1".equals(header(r, "x-dlq-attempt")), AWAIT);
            assertThat(republished).as("replayable record must be republished to the main topic").isNotNull();
            assertThat(header(republished, "x-dlq-reason")).as("reason dropped on republish").isNull();
            assertThat(header(republished, "x-dlq-attempt")).as("attempt incremented").isEqualTo("1");
            assertThat(header(republished, "x-message-id"))
                    .as("FRESH message_id so the inbox does not dedup the retry away")
                    .isNotNull().isNotEqualTo(originalMessageId);
            assertThat(header(republished, "x-replay-version")).as("fence input preserved").isEqualTo("5");
            assertThat(republished.value()).isEqualTo("command-bytes".getBytes(UTF_8));
        }
    }

    @Test
    void replayableReason_atAttemptBudget_isParkedAsRetryExhausted() {
        String key = "exhaust-" + UUID.randomUUID();
        String originalMessageId = UUID.randomUUID().toString();
        Map<String, String> headers = new HashMap<>();
        headers.put("x-dlq-reason", "STEP_TIMEOUT");
        headers.put("x-dlq-attempt", "5");   // == max-attempts → next would be 6 → exhausted
        headers.put("x-message-id", originalMessageId);
        headers.put("x-replay-version", "9");
        headers.put("x-tenant-id", UUID.randomUUID().toString());
        publish(COMMANDS_DLQ, key, "command-bytes".getBytes(UTF_8), headers);

        try (Consumer<String, byte[]> dlq = verificationConsumer(COMMANDS_DLQ)) {
            ConsumerRecord<String, byte[]> parked = awaitRecord(dlq,
                    r -> key.equals(r.key()) && "RETRY_EXHAUSTED".equals(header(r, "x-dlq-reason")), AWAIT);
            assertThat(parked).as("exhausted record must be parked as RETRY_EXHAUSTED").isNotNull();
            assertThat(header(parked, "x-message-id"))
                    .as("message_id kept for forensics on the terminal record").isEqualTo(originalMessageId);
        }
    }

    @Test
    void hardPoisonReason_isNeverRepublishedToMain() {
        // FENCING_VIOLATION is terminal — left in the DLQ, never republished. A trailing replayable
        // sentinel proves the consumer processed past the poison, so the poison's absence is meaningful.
        String poisonKey = "poison-" + UUID.randomUUID();
        Map<String, String> poison = new HashMap<>();
        poison.put("x-dlq-reason", "FENCING_VIOLATION");
        poison.put("x-dlq-attempt", "0");
        poison.put("x-message-id", UUID.randomUUID().toString());
        publish(COMMANDS_DLQ, poisonKey, "command-bytes".getBytes(UTF_8), poison);

        String sentinelKey = "sentinel-" + UUID.randomUUID();
        Map<String, String> sentinel = new HashMap<>();
        sentinel.put("x-dlq-reason", "INFRA_TRANSIENT");
        sentinel.put("x-dlq-attempt", "0");
        sentinel.put("x-message-id", UUID.randomUUID().toString());
        publish(COMMANDS_DLQ, sentinelKey, "command-bytes".getBytes(UTF_8), sentinel);

        try (Consumer<String, byte[]> main = verificationConsumer(COMMANDS)) {
            ConsumerRecord<String, byte[]> sentinelOnMain = awaitRecord(main, r -> sentinelKey.equals(r.key()), AWAIT);
            assertThat(sentinelOnMain)
                    .as("the trailing replayable sentinel must be republished (consumer is alive)").isNotNull();

            main.seekToBeginning(main.assignment());
            assertThat(scanKeys(main, Duration.ofSeconds(2)))
                    .as("hard poison must NOT be replayed to the main topic")
                    .doesNotContain(poisonKey);
        }
    }

    // --- assembly + IO helpers -----------------------------------------------------------------

    private static void startContainer(ConcurrentKafkaListenerContainerFactory<String, byte[]> factory,
                                       String topic, String groupId,
                                       AcknowledgingMessageListener<String, byte[]> listener) {
        ConcurrentMessageListenerContainer<String, byte[]> container = factory.createContainer(topic);
        container.getContainerProperties().setGroupId(groupId);
        container.getContainerProperties().setAckMode(AckMode.MANUAL);
        container.getContainerProperties().setMessageListener(listener);
        container.start();
        ContainerTestUtils.waitForAssignment(container, 1);
        CONTAINERS.add(container);
    }

    private static void createTopics() throws Exception {
        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic(COMMANDS, 1, (short) 1),
                    new NewTopic(COMMANDS_DLQ, 1, (short) 1))).all().get();
        }
    }

    private void publish(String topic, String key, byte[] value, Map<String, String> headers) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, value);
        headers.forEach((k, v) -> record.headers().add(k, v.getBytes(UTF_8)));
        try {
            template.send(record).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted publishing to " + topic, e);
        } catch (java.util.concurrent.ExecutionException e) {
            throw new IllegalStateException("failed publishing to " + topic, e);
        }
    }

    private Consumer<String, byte[]> verificationConsumer(String topic) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        Consumer<String, byte[]> consumer = new DefaultKafkaConsumerFactory<String, byte[]>(props).createConsumer();
        List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                .map(p -> new TopicPartition(p.topic(), p.partition()))
                .toList();
        consumer.assign(partitions);            // manual assignment — no group-join latency
        consumer.seekToBeginning(partitions);
        return consumer;
    }

    private ConsumerRecord<String, byte[]> awaitRecord(Consumer<String, byte[]> consumer,
                                                       Predicate<ConsumerRecord<String, byte[]>> match,
                                                       Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(200))) {
                if (match.test(record)) {
                    return record;
                }
            }
        }
        return null;
    }

    /** Polls until every requested key has been seen (or the deadline passes), keeping each record. */
    private Map<String, ConsumerRecord<String, byte[]>> awaitKeys(Consumer<String, byte[]> consumer,
                                                                  Set<String> keys, Duration timeout) {
        Map<String, ConsumerRecord<String, byte[]>> found = new HashMap<>();
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline && !found.keySet().containsAll(keys)) {
            for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(200))) {
                if (keys.contains(record.key())) {
                    found.put(record.key(), record);
                }
            }
        }
        return found;
    }

    private List<String> scanKeys(Consumer<String, byte[]> consumer, Duration window) {
        List<String> keys = new ArrayList<>();
        long deadline = System.currentTimeMillis() + window.toMillis();
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(200));
            records.forEach(r -> keys.add(r.key()));
        }
        return keys;
    }

    private static String header(ConsumerRecord<?, ?> record, String key) {
        Header h = record.headers().lastHeader(key);
        return h == null ? null : new String(h.value(), UTF_8);
    }
}
