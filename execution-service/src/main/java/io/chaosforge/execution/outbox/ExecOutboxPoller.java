package io.chaosforge.execution.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Execution Service result-event outbox relay (Defect B fix; ADR-0523 Phase 3 + ADR-0528 relay
 * semantics). A near-mirror of the CP {@code OutboxPoller}, kept local to {@code execution-service}
 * rather than extracted to a shared module — the two services are deliberately independent runtime
 * surfaces (architecture specifications: "do not collapse").
 *
 * <p>Each tick: (1) claims + leases a due batch under {@code FOR UPDATE SKIP LOCKED} in one short tx
 * that commits immediately; (2) dispatches the whole batch to Kafka <b>pipelined</b> (all sends in
 * flight), then harvests the futures against ONE shared deadline — a broker partition costs the tick
 * ~one send-timeout, not batch-size × send-timeout (arch-audit G2); (3) finalizes ALL outcomes in
 * ONE short tx of two batched statements — never a per-record finalize stampede (arch-audit A-3).
 *
 * <p>DEAD is gated on a record-fatal fault class ({@link PublishFaultClassifier}); broker-global
 * failures back off at the cap forever under the relay-lag alerts (arch-audit A-4). A crash between
 * claim and finalize leaves rows PENDING; they reappear after the lease (at-least-once; the
 * downstream consumer inbox dedups). Two lanes with complementary predicates (see
 * {@link ExecOutboxRelayDao}): the 1s hot lane claims rows younger than {@code claim-window-days};
 * the slow straggler lane claims older rows so nothing is unreachable before its partition drops.
 * A re-claim of another instance's still-PENDING row increments {@code outbox.lease_takeovers}.
 */
@Component
public class ExecOutboxPoller {

    private static final Logger log = LoggerFactory.getLogger(ExecOutboxPoller.class);

    private final ExecOutboxRelayDao dao;
    private final KafkaTemplate<String, byte[]> kafka;
    private final TransactionTemplate tx;
    private final Counter leaseTakeovers;
    private final String instanceId;

    @Value("${chaosforge.outbox.batch-size:64}")
    private int batchSize;
    @Value("${chaosforge.outbox.max-attempts:8}")
    private int maxAttempts;
    @Value("${chaosforge.outbox.lease-seconds:30}")   // visibility timeout > send-timeout (10s)
    private int leaseSeconds;
    // A partitioned broker never acks; ONE shared harvest deadline per pipelined batch bounds the tick.
    @Value("${chaosforge.outbox.send-timeout-ms:10000}")
    private long sendTimeoutMs;
    @Value("${chaosforge.outbox.base-backoff-seconds:2}")
    private double baseBackoffSeconds;
    @Value("${chaosforge.outbox.cap-backoff-seconds:300}")
    private double capBackoffSeconds;
    // Hot-lane freshness bound; MUST stay < chaosforge.partition.retention (exec inbox retention 7d,
    // outbox drop age) or rows would become straggler-only just as their partition drops.
    @Value("${chaosforge.outbox.claim-window-days:2}")
    private int claimWindowDays;
    @Value("${chaosforge.outbox.straggler-batch-size:16}")
    private int stragglerBatchSize;

    public ExecOutboxPoller(ExecOutboxRelayDao dao, KafkaTemplate<String, byte[]> execKafkaTemplate,
                            PlatformTransactionManager txManager, MeterRegistry registry) {
        this.dao = dao;
        this.kafka = execKafkaTemplate;
        this.tx = new TransactionTemplate(txManager);
        this.leaseTakeovers = registry.counter("outbox.lease_takeovers");
        this.instanceId = relayInstanceId();
    }

    @Scheduled(fixedDelayString = "${chaosforge.outbox.poll-interval-ms:1000}")
    public void poll() {
        publishAll(tx.execute(status ->
                dao.claimAndLease(batchSize, leaseSeconds, claimWindowDays, instanceId)));
    }

    /** Liveness lane for rows older than the hot-lane window — slow cadence, oldest first. */
    @Scheduled(fixedDelayString = "${chaosforge.outbox.straggler-poll-interval-ms:300000}")
    public void pollStragglers() {
        publishAll(tx.execute(status ->
                dao.claimAndLeaseStragglers(stragglerBatchSize, leaseSeconds, claimWindowDays, instanceId)));
    }

    private void publishAll(List<ExecOutboxRecord> batch) {
        if (batch == null || batch.isEmpty()) {
            return;
        }
        for (ExecOutboxRecord record : batch) {
            if (record.priorClaimedBy() != null && !record.priorClaimedBy().equals(instanceId)) {
                leaseTakeovers.increment();   // duplicate-publish window made visible (B-4)
            }
        }

        List<ExecOutboxRecord> sent = new ArrayList<>(batch.size());
        List<FailedPublish> failed = new ArrayList<>();
        List<InFlight> inFlight = new ArrayList<>(batch.size());

        // Dispatch pipelined. send() can throw/block synchronously (serialization, or buffer full /
        // metadata lost — bounded by max.block.ms): a record-fatal throw fails only that record; a
        // broker-global throw walls the producer, so fail the rest without re-hitting the same wall.
        boolean dispatchWalled = false;
        for (ExecOutboxRecord record : batch) {
            if (dispatchWalled) {
                failed.add(new FailedPublish(record, "producer_dispatch_walled", false));
                continue;
            }
            try {
                inFlight.add(new InFlight(record, kafka.send(toProducerRecord(record))));
            } catch (RuntimeException e) {
                boolean recordFatal = PublishFaultClassifier.isRecordFatal(e);
                failed.add(new FailedPublish(record, summarize(e), recordFatal));
                dispatchWalled = !recordFatal;
            }
        }

        // Harvest against ONE shared deadline: all sends already in flight, so total wait is
        // ~max(single send), bounded by sendTimeoutMs — not a per-record serial wait.
        long deadline = System.currentTimeMillis() + sendTimeoutMs;
        for (InFlight flight : inFlight) {
            long remainingMs = Math.max(1L, deadline - System.currentTimeMillis());
            try {
                flight.future().get(remainingMs, TimeUnit.MILLISECONDS);
                sent.add(flight.record());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();   // subsequent get() calls fail fast
                failed.add(new FailedPublish(flight.record(), "interrupted", false));
            } catch (ExecutionException | TimeoutException e) {
                failed.add(new FailedPublish(flight.record(), summarize(e),
                        PublishFaultClassifier.isRecordFatal(e)));
            }
        }

        // ONE finalize tx, two batched statements — a single connection per tick regardless of batch
        // size. If this tx itself fails, rows stay leased-PENDING and recover after the lease.
        tx.executeWithoutResult(status -> {
            dao.markSentBatch(sent);
            dao.markFailedOrDeadBatch(failed, maxAttempts, baseBackoffSeconds, capBackoffSeconds);
        });

        for (FailedPublish failure : failed) {
            if (failure.recordFatal() && failure.row().attempts() >= maxAttempts) {
                // Quarantined as DEAD — alert surface (outbox.dead_count gauge). Producer-side, not the DLQ.
                log.error("exec outbox row DEAD after {} attempts: message_id={} error={}",
                        failure.row().attempts(), failure.row().messageId(), failure.error());
            }
        }
    }

    // Purge of delivered rows is done by partition-drop (PartitionMaintenance, acceptance gate C28),
    // not a DELETE here — dropping an aged day-partition is O(1) and leaves no bloat for autovacuum.

    private static ProducerRecord<String, byte[]> toProducerRecord(ExecOutboxRecord r) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(r.topic(), r.partitionKey(), r.payload());
        record.headers()
                .add("x-message-id", r.messageId().toString().getBytes(UTF_8))
                .add("x-replay-version", Long.toString(r.replayVersion()).getBytes(UTF_8))
                .add("x-tenant-id", r.tenantId().toString().getBytes(UTF_8));
        return record;
    }

    /** Exception summary only — never the payload (PII surface). */
    private static String summarize(Throwable e) {
        String msg = e.getMessage();
        String detail = msg == null ? "" : (": " + (msg.length() > 200 ? msg.substring(0, 200) : msg));
        return e.getClass().getSimpleName() + detail;
    }

    /** Host + startup nonce: stable for this process, distinct across restarts and replicas. */
    private static String relayInstanceId() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            host = "unknown-host";
        }
        return host + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /** A dispatched record paired with its in-flight send future, awaiting harvest. */
    private record InFlight(ExecOutboxRecord record, CompletableFuture<SendResult<String, byte[]>> future) {}
}
