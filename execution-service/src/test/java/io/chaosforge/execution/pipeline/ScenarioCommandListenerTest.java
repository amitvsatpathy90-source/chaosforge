package io.chaosforge.execution.pipeline;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.chaosforge.execution.dlq.DlqRoutableException;
import io.chaosforge.execution.observability.ExecutionMetrics;
import io.chaosforge.schema.v1.ScenarioRunCommand;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.transaction.CannotCreateTransactionException;

/**
 * CHAOS-01 regression: a transient DB fault from any pipeline phase must route to <b>INFRA_TRANSIENT</b>
 * (replayable — the retry lane recovers it), NOT fall through {@code KafkaConsumerConfig.dlqReasonFor}'s
 * fail-closed {@code SCHEMA_INVALID} default (hard poison the retry consumer never touches). The two
 * injected classes are the exact ones a real fault produces: {@link DataAccessResourceFailureException}
 * (JdbcTemplate's translation of a PgJDBC {@code socketTimeout} / server {@code statement_timeout} — a
 * <em>NonTransient</em> subclass my first fix-draft's catch-list missed) and
 * {@link CannotCreateTransactionException} (Hikari connection-acquire failure at {@code @Transactional}
 * begin). A genuine DLQ reason ({@code FENCING_VIOLATION}) must pass through unchanged.
 */
class ScenarioCommandListenerTest {

    private final CommandDecoder decoder = mock(CommandDecoder.class);
    private final ClaimPhase claimPhase = mock(ClaimPhase.class);
    private final ExecutePhase executePhase = mock(ExecutePhase.class);
    private final FinalizePhase finalizePhase = mock(FinalizePhase.class);
    private final ExecutionMetrics metrics = mock(ExecutionMetrics.class);
    private final Acknowledgment ack = mock(Acknowledgment.class);

    private final ScenarioCommandListener listener =
            new ScenarioCommandListener(decoder, claimPhase, executePhase, finalizePhase, metrics);

    private final UUID scenarioId = UUID.randomUUID();
    private final UUID tenantId = UUID.randomUUID();
    private final UUID messageId = UUID.randomUUID();

    private ConsumerRecord<String, byte[]> record() {
        ConsumerRecord<String, byte[]> record =
                new ConsumerRecord<>("chaosforge.scenario.commands.v1", 0, 0L, scenarioId.toString(), new byte[]{1});
        record.headers().add("x-tenant-id", tenantId.toString().getBytes(UTF_8));
        record.headers().add("x-message-id", messageId.toString().getBytes(UTF_8));
        record.headers().add("x-replay-version", "5".getBytes(UTF_8));
        when(decoder.decode(any())).thenReturn(mock(ScenarioRunCommand.class));
        return record;
    }

    @Test
    void dbFaultInClaim_routesInfraTransient_notSchemaInvalid() {
        ConsumerRecord<String, byte[]> record = record();
        when(claimPhase.claim(any(), any(), anyLong()))
                .thenThrow(new DataAccessResourceFailureException("socketTimeout mid-query"));

        assertThatThrownBy(() -> listener.onCommand(record, ack))
                .isInstanceOfSatisfying(DlqRoutableException.class,
                        e -> assertThat(e.dlqReason()).isEqualTo("INFRA_TRANSIENT"))
                .satisfies(e -> assertThat(((DlqRoutableException) e).replayable()).isTrue());
        verify(ack, never()).acknowledge();   // no ack → the record is redelivered/DLQ'd, not lost
    }

    @Test
    void dbFaultInFinalize_routesInfraTransient() {
        ConsumerRecord<String, byte[]> record = record();
        when(claimPhase.claim(any(), any(), anyLong())).thenReturn(ClaimResult.proceed());
        when(executePhase.execute(any(), anyLong()))
                .thenReturn(new ExecutionResult(scenarioId, tenantId, 5L, "COMPLETED"));
        doThrow(new CannotCreateTransactionException("Hikari acquire timeout at tx begin"))
                .when(finalizePhase).complete(any());

        assertThatThrownBy(() -> listener.onCommand(record, ack))
                .isInstanceOfSatisfying(DlqRoutableException.class,
                        e -> assertThat(e.dlqReason()).isEqualTo("INFRA_TRANSIENT"));
        verify(ack, never()).acknowledge();
    }

    @Test
    void fencingViolationFromClaim_isNotReclassified() {
        ConsumerRecord<String, byte[]> record = record();
        when(claimPhase.claim(any(), any(), anyLong()))
                .thenThrow(DlqRoutableException.fencingViolation("stale replay_version 3 < max_seen 4"));

        // A real DLQ reason must survive: the DB-fault catch is DataAccessException|TransactionException,
        // and DlqRoutableException is neither, so it propagates to FENCING_VIOLATION untouched.
        assertThatThrownBy(() -> listener.onCommand(record, ack))
                .isInstanceOfSatisfying(DlqRoutableException.class,
                        e -> assertThat(e.dlqReason()).isEqualTo("FENCING_VIOLATION"));
        verify(ack, never()).acknowledge();
    }

    @Test
    void duplicate_isAckSkipped_withoutExecuting() {
        ConsumerRecord<String, byte[]> record = record();
        when(claimPhase.claim(any(), any(), anyLong())).thenReturn(ClaimResult.duplicate());

        listener.onCommand(record, ack);

        verify(executePhase, never()).execute(any(), anyLong());
        verify(ack).acknowledge();
    }

    @Test
    void happyPath_finalizesThenAcks() {
        ConsumerRecord<String, byte[]> record = record();
        when(claimPhase.claim(any(), any(), anyLong())).thenReturn(ClaimResult.proceed());
        when(executePhase.execute(any(), anyLong()))
                .thenReturn(new ExecutionResult(scenarioId, tenantId, 5L, "COMPLETED"));

        listener.onCommand(record, ack);

        verify(finalizePhase).complete(any());
        verify(metrics).success();
        verify(ack).acknowledge();
    }
}
