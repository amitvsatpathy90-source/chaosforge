package io.chaosforge.execution.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.chaosforge.execution.outbox.ExecOutboxPoller;
import io.chaosforge.execution.outbox.ExecOutboxRelayDao;
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
 * Execution Service result-event outbox relay (Defect B) against real Postgres with a mock broker.
 * Proves the relay ships a Phase-3 result row to Kafka and flips it SENT only post-broker-ack, that a
 * publish failure leaves the row PENDING and recoverable (never lost, never stuck) — the crash
 * contract of ADR-0528 — and the arch-audit reconciliation invariants:
 *
 * <ul>
 *   <li><b>DEAD is record-fatal-only (A-4):</b> a broker-global failure at/over max-attempts stays
 *       PENDING on backoff — a broker outage of any length never mass-DEADs the outbox;</li>
 *   <li><b>the tick is bounded by ONE shared deadline (G2):</b> a never-acking broker costs a
 *       pipelined batch ~one send-timeout, not batch-size × send-timeout;</li>
 *   <li><b>a lease takeover is a counter, not an inference (B-4).</b></li>
 * </ul>
 */
class ExecOutboxRelayIT extends ExecPostgresIT {

    @Test
    @SuppressWarnings("unchecked")
    void pendingResultEvent_isPublished_thenFlippedSent() {
        UUID messageId = UUID.randomUUID();
        insertPendingResultEvent(messageId);

        KafkaTemplate<String, byte[]> kafka = mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));   // broker ack

        ExecOutboxPoller poller = newPoller(kafka);
        poller.poll();

        assertThat(status(messageId)).isEqualTo("SENT");
        assertThat(sentAt(messageId)).isNotNull();
        verify(kafka, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void publishFailure_leavesRowPending_thenNextPollSendsIt() {
        UUID messageId = UUID.randomUUID();
        insertPendingResultEvent(messageId);

        KafkaTemplate<String, byte[]> kafka = mock(KafkaTemplate.class);
        CompletableFuture<SendResult<String, byte[]>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker down"));   // simulate crash mid-publish
        when(kafka.send(any(ProducerRecord.class)))
                .thenReturn(failed)                                            // first poll: publish fails
                .thenReturn(CompletableFuture.completedFuture(null));          // second poll: broker ack

        ExecOutboxPoller poller = newPoller(kafka);

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
    void partitionedBroker_sendNeverAcks_pollBoundedBySendTimeout_rowStaysPending() {
        // Kafka-partition (acceptance gate C19): the broker accepts the connection but never acks the
        // send. The poll must NOT hang on that one record — it aborts the wait at send-timeout, backs
        // the row off to PENDING, and stays recoverable. Modeled by a future that never completes.
        UUID messageId = UUID.randomUUID();
        insertPendingResultEvent(messageId);

        KafkaTemplate<String, byte[]> kafka = mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class)))
                .thenReturn(new CompletableFuture<>());   // never completes → models a partitioned broker

        ExecOutboxPoller poller = newPoller(kafka);
        ReflectionTestUtils.setField(poller, "sendTimeoutMs", 500L);   // short bound for the test

        long startMs = System.currentTimeMillis();
        poller.poll();   // must return — bounded by the send timeout, not an infinite wait
        long elapsedMs = System.currentTimeMillis() - startMs;

        assertThat(elapsedMs).as("poll aborts the hung send at ~send-timeout, never hangs").isLessThan(5_000L);
        assertThat(status(messageId)).as("the un-acked row survives as PENDING, recoverable").isEqualTo("PENDING");
        assertThat(attempts(messageId)).isEqualTo(1);
        assertThat(sentAt(messageId)).isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    void pipelinedBatch_neverAckingBroker_boundedByOneSharedDeadline_notPerRecord() {
        // Arch-audit G2 regression: pre-fix the relay awaited each send serially (batch × timeout);
        // post-fix all sends are dispatched pipelined and harvested against ONE shared deadline, so a
        // partitioned broker costs the whole batch ~one send-timeout. 8 rows at 1s timeout: the old
        // serial path needs ≥ 8s — the pipelined path must finish in well under 3s.
        int batch = 8;
        UUID[] ids = new UUID[batch];
        for (int i = 0; i < batch; i++) {
            ids[i] = UUID.randomUUID();
            insertPendingResultEvent(ids[i]);
        }

        KafkaTemplate<String, byte[]> kafka = mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class)))
                .thenReturn(new CompletableFuture<>());   // never completes, for every record

        ExecOutboxPoller poller = newPoller(kafka);
        ReflectionTestUtils.setField(poller, "sendTimeoutMs", 1_000L);

        long startMs = System.currentTimeMillis();
        poller.poll();
        long elapsedMs = System.currentTimeMillis() - startMs;

        assertThat(elapsedMs)
                .as("pipelined harvest: ~1 shared deadline for the whole batch, not %sx serial waits", batch)
                .isLessThan(3_000L);
        for (UUID id : ids) {
            assertThat(status(id)).as("every un-acked row survives as PENDING").isEqualTo("PENDING");
        }
        verify(kafka, times(batch)).send(any(ProducerRecord.class));   // all dispatched (pipelined)
    }

    @Test
    @SuppressWarnings("unchecked")
    void brokerOutage_atMaxAttempts_staysPending_neverDead() {
        // Arch-audit A-4 regression (the DEAD-ratchet): pre-fix ANY failure at max-attempts flipped the
        // row DEAD — so a ~10-minute broker outage mass-DEADed every in-flight row, and recovery meant
        // manual UPDATEs (the exact data-repair the C31 game-day bar forbids). Post-fix a broker-global
        // failure NEVER quarantines: the row stays PENDING on capped backoff under the relay-lag alerts.
        UUID messageId = UUID.randomUUID();
        insertPendingResultEvent(messageId);

        KafkaTemplate<String, byte[]> kafka = mock(KafkaTemplate.class);
        CompletableFuture<SendResult<String, byte[]>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker unreachable"));   // broker-global fault
        when(kafka.send(any(ProducerRecord.class))).thenReturn(failed);

        ExecOutboxPoller poller = newPoller(kafka);
        ReflectionTestUtils.setField(poller, "maxAttempts", 1);   // budget exhausted on the first failure

        poller.poll();

        assertThat(status(messageId))
                .as("a broker outage must never DEAD a row — it backs off and outlives the outage")
                .isEqualTo("PENDING");
        assertThat(new ExecOutboxRelayDao(jdbc).deadCount()).isZero();
    }

    @Test
    @SuppressWarnings("unchecked")
    void recordFatalRow_isQuarantinedDead_afterMaxAttempts_andNotRetried() {
        // The one legitimate road to DEAD: a fault that retrying the same bytes can never fix.
        UUID messageId = UUID.randomUUID();
        insertPendingResultEvent(messageId);

        KafkaTemplate<String, byte[]> kafka = mock(KafkaTemplate.class);
        CompletableFuture<SendResult<String, byte[]>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RecordTooLargeException("record exceeds max.message.bytes"));
        when(kafka.send(any(ProducerRecord.class))).thenReturn(failed);   // always fails, record-fatally

        ExecOutboxPoller poller = newPoller(kafka);
        ReflectionTestUtils.setField(poller, "maxAttempts", 1);   // first record-fatal attempt → DEAD

        poller.poll();   // attempt #1 fails record-fatally; attempts(1) >= maxAttempts(1) → DEAD

        assertThat(status(messageId)).as("record-fatal poison quarantined, not retried forever").isEqualTo("DEAD");
        assertThat(new ExecOutboxRelayDao(jdbc).deadCount()).isEqualTo(1L);
        // DEAD rows are terminal — a further poll must not pick them up (status != PENDING).
        poller.poll();
        verify(kafka, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void leaseTakeover_reclaimByAnotherInstance_incrementsCounter() {
        // Arch-audit B-4: the duplicate-publish window (instance B re-claims a row instance A leased
        // but never finalized) is an observable counter, not an inference from logs.
        UUID messageId = UUID.randomUUID();
        insertPendingResultEvent(messageId);

        KafkaTemplate<String, byte[]> neverAcks = mock(KafkaTemplate.class);
        when(neverAcks.send(any(ProducerRecord.class))).thenReturn(new CompletableFuture<>());
        SimpleMeterRegistry registryA = new SimpleMeterRegistry();
        ExecOutboxPoller pollerA = newPoller(neverAcks, registryA);
        ReflectionTestUtils.setField(pollerA, "sendTimeoutMs", 200L);

        pollerA.poll();   // A claims (writes claimed_by=A), publish times out, row backs off PENDING
        assertThat(registryA.get("outbox.lease_takeovers").counter().count())
                .as("first claim has no prior claimant").isZero();

        // Lease expiry / backoff elapsed — the row is due again.
        jdbc.update("UPDATE outbox SET next_attempt_at = now() WHERE message_id = ?", messageId);

        KafkaTemplate<String, byte[]> acks = mock(KafkaTemplate.class);
        when(acks.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        SimpleMeterRegistry registryB = new SimpleMeterRegistry();
        ExecOutboxPoller pollerB = newPoller(acks, registryB);   // distinct startup nonce → different id

        pollerB.poll();   // B re-claims A's still-PENDING row → takeover observed, then ships it

        assertThat(registryB.get("outbox.lease_takeovers").counter().count())
                .as("re-claim of another instance's leased-but-unfinalized row is a takeover").isEqualTo(1.0d);
        assertThat(status(messageId)).isEqualTo("SENT");
    }

    @Test
    @SuppressWarnings("unchecked")
    void finalizePhaseRow_isPickedUpAndShipped_endToEnd() {
        // Closes the Defect B loop: the real Phase-3 producer of the row → the relay that ships it.
        // Phase 3 only ever runs after Phase 1 claimed the run, so seed the IN_PROGRESS header first —
        // the status-guarded finalize (arch-audit H2) emits the event only when it wins that flip.
        UUID scenarioId = UUID.randomUUID();
        UUID tenantId = UUID.randomUUID();
        io.chaosforge.execution.persistence.RunLogDao runLogDao =
                new io.chaosforge.execution.persistence.RunLogDao(jdbc);
        runLogDao.insertRunHeader(scenarioId, 7L, tenantId);
        tx.executeWithoutResult(s -> new io.chaosforge.execution.pipeline.FinalizePhase(
                new io.chaosforge.execution.persistence.ExecOutboxDao(jdbc),
                runLogDao,
                new io.chaosforge.execution.pipeline.AvroResultPayloadCodec())
                .complete(new io.chaosforge.execution.pipeline.ExecutionResult(
                        scenarioId, tenantId, 7L, "COMPLETED")));

        // Phase 3 wrote exactly one PENDING result-event row for this scenario.
        Integer pending = jdbc.queryForObject(
                "SELECT count(*) FROM outbox WHERE aggregate_id = ? AND status = 'PENDING' "
                        + "AND topic = 'chaosforge.scenario.results.v1' AND partition_key = ?",
                Integer.class, scenarioId, scenarioId.toString());
        assertThat(pending).isEqualTo(1);

        KafkaTemplate<String, byte[]> kafka = mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        newPoller(kafka).poll();

        Integer stillPending = jdbc.queryForObject(
                "SELECT count(*) FROM outbox WHERE aggregate_id = ? AND status = 'PENDING'",
                Integer.class, scenarioId);
        assertThat(stillPending).as("relay shipped the Phase-3 result event").isZero();
        verify(kafka, times(1)).send(any(ProducerRecord.class));
    }

    private ExecOutboxPoller newPoller(KafkaTemplate<String, byte[]> kafka) {
        return newPoller(kafka, new SimpleMeterRegistry());
    }

    private ExecOutboxPoller newPoller(KafkaTemplate<String, byte[]> kafka, SimpleMeterRegistry registry) {
        ExecOutboxPoller poller = new ExecOutboxPoller(
                new ExecOutboxRelayDao(jdbc), kafka, new DataSourceTransactionManager(ds), registry);
        ReflectionTestUtils.setField(poller, "batchSize", 64);
        ReflectionTestUtils.setField(poller, "maxAttempts", 8);
        ReflectionTestUtils.setField(poller, "leaseSeconds", 30);
        ReflectionTestUtils.setField(poller, "baseBackoffSeconds", 2.0);
        ReflectionTestUtils.setField(poller, "capBackoffSeconds", 300.0);
        ReflectionTestUtils.setField(poller, "sendTimeoutMs", 10_000L);   // production default; overridden per-test
        ReflectionTestUtils.setField(poller, "claimWindowDays", 2);       // production default (yml)
        ReflectionTestUtils.setField(poller, "stragglerBatchSize", 16);
        return poller;
    }

    private void insertPendingResultEvent(UUID messageId) {
        jdbc.update("INSERT INTO outbox (message_id, aggregate_id, tenant_id, topic, partition_key, "
                        + "replay_version, payload) VALUES (?, ?, ?, ?, ?, ?, ?)",
                messageId, UUID.randomUUID(), UUID.randomUUID(), "chaosforge.scenario.results.v1",
                messageId.toString(), 1L, new byte[] {0x1});
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
