package io.chaosforge.controlplane.pipeline;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.chaosforge.controlplane.repository.RunProjectionRepository;
import io.chaosforge.schema.v1.ScenarioRunResult;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.kafka.support.Acknowledgment;

class RunProjectionListenerTest {

    private final ResultPayloadDecoder decoder = mock(ResultPayloadDecoder.class);
    private final RunProjectionRepository repository = mock(RunProjectionRepository.class);
    private final RunProjectionMetrics metrics = mock(RunProjectionMetrics.class);
    private final Acknowledgment ack = mock(Acknowledgment.class);
    private final RunProjectionListener listener = new RunProjectionListener(decoder, repository, metrics);

    @Test
    void malformedBytes_countsSkipsAndAcks_neverTouchesRepository() {
        byte[] payload = {0x00, (byte) 0xFF};
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>("chaosforge.scenario.results.v1", 0, 1L,
                "key", payload);
        when(decoder.decode(payload)).thenThrow(new IllegalStateException("bad bytes"));

        listener.onResult(record, ack);

        verify(metrics).decodeFailure();
        verify(ack).acknowledge();
        verifyNoInteractions(repository);
    }

    @Test
    void persistFailure_countsAndRethrowsWithoutAck_soTheErrorHandlerCanRetry() {
        byte[] payload = {0x01};
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>("chaosforge.scenario.results.v1", 0, 2L,
                "key", payload);
        ScenarioRunResult result = ScenarioRunResult.newBuilder()
                .setScenarioId(UUID.randomUUID().toString())
                .setTenantId(UUID.randomUUID().toString())
                .setReplayVersion(1L)
                .setOutcome("COMPLETED")
                .setFinishedAt(Instant.now())
                .build();
        when(decoder.decode(payload)).thenReturn(result);
        doThrow(new DataAccessResourceFailureException("pool exhausted"))
                .when(repository).upsert(any(), anyLong(), any(), any(), any());

        assertThatThrownBy(() -> listener.onResult(record, ack))
                .isInstanceOf(DataAccessResourceFailureException.class);

        verify(metrics).persistFailure();
        verify(ack, never()).acknowledge();
    }
}
