package io.chaosforge.controlplane.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.chaosforge.controlplane.outbox.OutboxPoller;
import io.chaosforge.controlplane.outbox.OutboxRelayDao;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Outbox crash/restart (ADR-0528): a publish that fails (broker down / crash before the SENT-flip
 * commits) leaves the row PENDING and recoverable — never lost, never stuck. On the next poll it is
 * republished and only then flipped to SENT. The consumer inbox dedups the at-least-once delivery.
 *
 * <p>DEAD is gated on a record-fatal fault (arch-audit A-4): a broker-global failure never
 * quarantines a row, no matter how many attempts — a broker outage must never force manual
 * data-repair writes (the C31 bar). Only a fault retrying the same bytes cannot fix goes DEAD.
 */
class OutboxPollerCrashIT extends CpPostgresIT {

    @Test
    @SuppressWarnings("unchecked")
    void publishFailure_leavesRowPending_thenNextPollSendsIt() {
        UUID messageId = UUID.randomUUID();
        insertPendingOutbox(messageId);

        KafkaTemplate<String, byte[]> kafka = mock(KafkaTemplate.class);
        CompletableFuture<SendResult<String, byte[]>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker down"));   // simulate crash mid-publish
        when(kafka.send(any(ProducerRecord.class)))
                .thenReturn(failed)                                // first poll: publish fails
                .thenReturn(CompletableFuture.completedFuture(null));   // second poll: broker ack

        OutboxPoller poller = newPoller(kafka);

        poller.poll();   // attempt #1 — fails
        assertThat(status(messageId)).as("survives as PENDING after a failed publish").isEqualTo("PENDING");
        assertThat(attempts(messageId)).isEqualTo(1);
        assertThat(sentAt(messageId)).isNull();

        // The failure backed next_attempt_at off; fast-forward it so the row is due again ("restart").
        jdbc.update("UPDATE outbox SET next_attempt_at = now() WHERE message_id = ?", messageId);

        poller.poll();   // attempt #2 — broker ack
        assertThat(status(messageId)).as("republished and flipped to SENT").isEqualTo("SENT");
        assertThat(sentAt(messageId)).isNotNull();
        verify(kafka, times(2)).send(any(ProducerRecord.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void brokerOutage_atMaxAttempts_staysPending_neverDead() {
        // Arch-audit A-4 regression (the DEAD-ratchet): pre-fix ANY failure at max-attempts flipped the
        // row DEAD, so a ~10-minute broker outage mass-DEADed every in-flight command and recovery
        // required manual UPDATEs. Post-fix a broker-global failure NEVER quarantines: the row stays
        // PENDING on capped backoff under the OutboxRelayLagging / oldest_pending_age alerts.
        UUID messageId = UUID.randomUUID();
        insertPendingOutbox(messageId);

        KafkaTemplate<String, byte[]> kafka = mock(KafkaTemplate.class);
        CompletableFuture<SendResult<String, byte[]>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker unreachable"));   // broker-global fault
        when(kafka.send(any(ProducerRecord.class))).thenReturn(failed);

        OutboxPoller poller = newPoller(kafka);
        ReflectionTestUtils.setField(poller, "maxAttempts", 1);   // budget exhausted on the first failure

        poller.poll();

        assertThat(status(messageId))
                .as("a broker outage must never DEAD a row — it backs off and outlives the outage")
                .isEqualTo("PENDING");
        assertThat(new OutboxRelayDao(jdbc).deadCount()).isZero();
    }

    @Test
    @SuppressWarnings("unchecked")
    void recordFatalRow_isQuarantinedDead_afterMaxAttempts_andNotRetried() {
        // The one legitimate road to DEAD: a fault that retrying the same bytes can never fix.
        UUID messageId = UUID.randomUUID();
        insertPendingOutbox(messageId);

        KafkaTemplate<String, byte[]> kafka = mock(KafkaTemplate.class);
        CompletableFuture<SendResult<String, byte[]>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RecordTooLargeException("record exceeds max.message.bytes"));
        when(kafka.send(any(ProducerRecord.class))).thenReturn(failed);   // always fails, record-fatally

        OutboxPoller poller = newPoller(kafka);
        ReflectionTestUtils.setField(poller, "maxAttempts", 1);   // first record-fatal attempt → DEAD (V8 terminal)

        poller.poll();   // attempt #1 fails record-fatally; attempts(1) >= maxAttempts(1) → DEAD

        assertThat(status(messageId)).as("record-fatal poison quarantined, not retried forever").isEqualTo("DEAD");
        assertThat(new OutboxRelayDao(jdbc).deadCount()).isEqualTo(1L);

        // DEAD is terminal — the claim query (status='PENDING') must never pick it up again.
        poller.poll();
        verify(kafka, times(1)).send(any(ProducerRecord.class));
    }

    private OutboxPoller newPoller(KafkaTemplate<String, byte[]> kafka) {
        OutboxPoller poller = new OutboxPoller(
                new OutboxRelayDao(jdbc), kafka, new DataSourceTransactionManager(ds), new SimpleMeterRegistry());
        ReflectionTestUtils.setField(poller, "batchSize", 64);
        ReflectionTestUtils.setField(poller, "maxAttempts", 8);
        ReflectionTestUtils.setField(poller, "leaseSeconds", 30);
        ReflectionTestUtils.setField(poller, "baseBackoffSeconds", 2.0);
        ReflectionTestUtils.setField(poller, "capBackoffSeconds", 300.0);
        ReflectionTestUtils.setField(poller, "sendTimeoutMs", 10_000L);
        ReflectionTestUtils.setField(poller, "claimWindowDays", 2);   // production default (yml)
        ReflectionTestUtils.setField(poller, "stragglerBatchSize", 16);
        return poller;
    }

    private void insertPendingOutbox(UUID messageId) {
        jdbc.update("INSERT INTO outbox (message_id, aggregate_id, tenant_id, topic, partition_key, "
                        + "replay_version, rule_set_id, rule_set_version, payload) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                messageId, UUID.randomUUID(), UUID.randomUUID(), "chaosforge.scenario.commands.v1",
                messageId.toString(), 1L, UUID.randomUUID(), 1, new byte[] {0x1});
    }

    private String status(UUID messageId) {
        return jdbc.queryForObject("SELECT status FROM outbox WHERE message_id = ?", String.class, messageId);
    }

    private int attempts(UUID messageId) {
        Integer a = jdbc.queryForObject("SELECT attempts FROM outbox WHERE message_id = ?", Integer.class, messageId);
        return a == null ? -1 : a;
    }

    private Object sentAt(UUID messageId) {
        return jdbc.queryForObject("SELECT sent_at FROM outbox WHERE message_id = ?", Object.class, messageId);
    }
}
