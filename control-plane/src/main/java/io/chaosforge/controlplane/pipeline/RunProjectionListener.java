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

/** Projects terminal ScenarioRunResult events into run_projection for get_run_status. */
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
            ack.acknowledge();
        } catch (RuntimeException e) {
            // Transient (DB down, pool exhausted) — don't ack; let Kafka redeliver.
            log.warn("run_projection persist failed, will retry: partition={} offset={}",
                    record.partition(), record.offset(), e);
            metrics.persistFailure();
        }
    }
}
