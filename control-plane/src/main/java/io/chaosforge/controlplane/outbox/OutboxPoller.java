package io.chaosforge.controlplane.outbox;

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
 * Outbox relay (ADR-0528). Per tick: claim+lease a batch (SKIP LOCKED, short tx), dispatch
 * pipelined to Kafka against one shared harvest deadline, finalize all outcomes in one batched tx.
 * Pipelining is ordering-safe: idempotent producer + fencing/inbox dedup absorb reorder.
 * DEAD requires a record-fatal fault ({@link PublishFaultClassifier}); broker-global failures
 * back off at the cap forever. Crash between claim/finalize leaves rows PENDING for lease retry.
 * Hot lane (claim-window-days) + straggler lane cover all ages; multi-poller safe via lease takeover.
 */
@Component
public class OutboxPoller {

    private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);

    private final OutboxRelayDao dao;
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
    @Value("${chaosforge.outbox.send-timeout-ms:10000}")   // ONE shared harvest deadline per tick
    private long sendTimeoutMs;
    @Value("${chaosforge.outbox.base-backoff-seconds:2}")
    private double baseBackoffSeconds;
    @Value("${chaosforge.outbox.cap-backoff-seconds:300}")
    private double capBackoffSeconds;
    // Must stay < outbox-retention-days or rows go straggler-only as their partition drops.
    @Value("${chaosforge.outbox.claim-window-days:2}")
    private int claimWindowDays;
    @Value("${chaosforge.outbox.straggler-batch-size:16}")
    private int stragglerBatchSize;

    public OutboxPoller(OutboxRelayDao dao, KafkaTemplate<String, byte[]> outboxKafkaTemplate,
                        PlatformTransactionManager txManager, MeterRegistry registry) {
        this.dao = dao;
        this.kafka = outboxKafkaTemplate;
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

    private void publishAll(List<OutboxRecord> batch) {
        if (batch == null || batch.isEmpty()) {
            return;
        }
        for (OutboxRecord record : batch) {
            if (record.priorClaimedBy() != null && !record.priorClaimedBy().equals(instanceId)) {
                leaseTakeovers.increment();   // duplicate-publish window made visible (B-4)
            }
        }

        List<OutboxRecord> sent = new ArrayList<>(batch.size());
        List<FailedPublish> failed = new ArrayList<>();
        List<InFlight> inFlight = new ArrayList<>(batch.size());

        // Record-fatal throw fails only that record; broker-global throw walls the rest of the batch.
        boolean dispatchWalled = false;
        for (OutboxRecord record : batch) {
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

        // Harvest against one shared deadline — not a per-record serial wait.
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

        // One finalize tx, one connection per tick. Failure leaves rows leased-PENDING for lease recovery.
        tx.executeWithoutResult(status -> {
            dao.markSentBatch(sent);
            dao.markFailedOrDeadBatch(failed, maxAttempts, baseBackoffSeconds, capBackoffSeconds);
        });

        for (FailedPublish failure : failed) {
            if (failure.recordFatal() && failure.row().attempts() >= maxAttempts) {
                // Quarantined as DEAD (outbox.dead_count gauge) — producer-side, not the DLQ.
                log.error("outbox row DEAD after {} attempts: message_id={} error={}",
                        failure.row().attempts(), failure.row().messageId(), failure.error());
            }
        }
    }

    // Purge of delivered rows is done by partition-drop (PartitionMaintenance, acceptance gate C28),
    // not a DELETE here — dropping an aged day-partition is O(1) and leaves no bloat for autovacuum.

    private static ProducerRecord<String, byte[]> toProducerRecord(OutboxRecord r) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(r.topic(), r.partitionKey(), r.payload());
        record.headers()
                .add("x-message-id", r.messageId().toString().getBytes(UTF_8))
                .add("x-replay-version", Long.toString(r.replayVersion()).getBytes(UTF_8))
                .add("x-rule-set-id", r.ruleSetId().toString().getBytes(UTF_8))
                .add("x-rule-set-version", Integer.toString(r.ruleSetVersion()).getBytes(UTF_8))
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
    private record InFlight(OutboxRecord record, CompletableFuture<SendResult<String, byte[]>> future) {}
}
