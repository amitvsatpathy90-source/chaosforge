package io.chaosforge.controlplane.pipeline;

import io.chaosforge.controlplane.repository.RunProjectionRepository;
import io.chaosforge.schema.v1.ScenarioRunResult;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Projects terminal {@code ScenarioRunResult} events from the results topic into
 * {@code run_projection}, the read cache behind the {@code get_run_status} MCP tool.
 *
 * <p>The projection is rebuildable, so failures are counted and skipped, never DLQ-routed.
 * A record that fails to decode is logged, counted and acked, since retry cannot help.
 * A failed upsert is rethrown so the container error handler retries it (2 retries, 2 s apart),
 * then counts it as exhausted and moves on. The upsert is idempotent on
 * {@code (scenario_id, replay_version)}, so redelivery is safe. Acks are manual.
 */
@Component
public class RunProjectionListener {

    private static final Logger log = LoggerFactory.getLogger(RunProjectionListener.class);

    private final ResultPayloadDecoder decoder;
    private final RunProjectionRepository repository;
    private final RunProjectionMetrics metrics;

    public RunProjectionListener(ResultPayloadDecoder decoder, RunProjectionRepository repository,
                                 RunProjectionMetrics metrics) {
        this.decoder = decoder;
        this.repository = repository;
        this.metrics = metrics;
    }

    // Manual ack: only after decode-skip or a successful upsert (AckMode.MANUAL).
    @KafkaListener(topics = "${chaosforge.kafka.results-topic}",
            containerFactory = "runProjectionListenerContainerFactory")
    public void onResult(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        ScenarioRunResult result;
        try {
            result = decoder.decode(record.value());
        } catch (RuntimeException e) {
            // Bad bytes are permanently bad — log, count, skip, and ack. Retry can't help.
            log.warn("run_projection decode skip: partition={} offset={}", record.partition(), record.offset(), e);
            metrics.decodeFailure();
            ack.acknowledge();
            return;
        }

        try {
            repository.upsert(
                    UUID.fromString(result.getScenarioId().toString()),
                    result.getReplayVersion(),
                    UUID.fromString(result.getTenantId().toString()),
                    result.getOutcome().toString(),
                    result.getFinishedAt());
        } catch (RuntimeException e) {
            log.warn("run_projection persist failed, will retry: partition={} offset={}",
                    record.partition(), record.offset(), e);
            metrics.persistFailure();
            // Rethrow — the container's DefaultErrorHandler (KafkaConsumerConfig) seeks back to
            // this record and retries with backoff. Swallowing here and just not acking would NOT
            // give a prompt, deterministic retry under AckMode.MANUAL — only a later restart or
            // rebalance would ever pick it back up.
            throw e;
        }
        ack.acknowledge();
    }
}
