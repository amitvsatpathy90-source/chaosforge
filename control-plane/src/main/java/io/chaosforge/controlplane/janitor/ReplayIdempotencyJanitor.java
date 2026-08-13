package io.chaosforge.controlplane.janitor;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Scheduled GC for {@code replay_idempotency} (ADR-0528 §TTL janitor) — reclaims expired rows in
 * bounded, index-backed chunks off-peak. Chunked deletes keep locks short; per-run cap prevents
 * monopolizing a connection; AtomicBoolean CAS skips overlap; SKIP LOCKED is multi-instance-safe.
 * DataAccessException is caught and logged, never rethrown — committed chunks survive a mid-run failure.
 */
@Component
public class ReplayIdempotencyJanitor {

    private static final Logger log = LoggerFactory.getLogger(ReplayIdempotencyJanitor.class);

    private static final String DELETED = "chaosforge.replay_idempotency.gc.deleted";
    private static final String RUNS = "chaosforge.replay_idempotency.gc.runs";

    private final ReplayIdempotencyGcDao dao;
    private final Counter deletedRows;
    private final Counter runsOk;
    private final Counter runsFailed;
    private final Counter runsSkipped;

    /** Guards against overlapping executions within this JVM. */
    private final AtomicBoolean running = new AtomicBoolean(false);

    @Value("${chaosforge.replay-idempotency.gc.retention-hours:24}")
    private int retentionHours;
    @Value("${chaosforge.replay-idempotency.gc.chunk-size:500}")
    private int chunkSize;
    @Value("${chaosforge.replay-idempotency.gc.max-chunks-per-run:1000}")
    private int maxChunksPerRun;

    public ReplayIdempotencyJanitor(ReplayIdempotencyGcDao dao, MeterRegistry registry) {
        this.dao = dao;
        this.deletedRows = Counter.builder(DELETED)
                .description("replay_idempotency rows reclaimed by the TTL janitor (ADR-0528)")
                .register(registry);
        this.runsOk = Counter.builder(RUNS).tag("outcome", "success").register(registry);
        this.runsFailed = Counter.builder(RUNS).tag("outcome", "failed").register(registry);
        this.runsSkipped = Counter.builder(RUNS).tag("outcome", "skipped").register(registry);
    }

    /** Off-peak by default (04:00). Cron is configurable; retention/chunking are independent of it. */
    @Scheduled(cron = "${chaosforge.replay-idempotency.gc.cron:0 0 4 * * *}")
    public void purgeExpired() {
        if (!running.compareAndSet(false, true)) {
            runsSkipped.increment();
            log.warn("replay_idempotency GC skipped — a previous run is still in flight");
            return;
        }
        long startNanos = System.nanoTime();
        long totalDeleted = 0;
        try {
            for (int chunk = 0; chunk < maxChunksPerRun; chunk++) {
                int deleted = dao.deleteExpiredChunk(retentionHours, chunkSize);
                totalDeleted += deleted;
                if (deleted < chunkSize) {
                    break;   // fewer than a full chunk ⇒ the expired set is drained
                }
            }
            deletedRows.increment(totalDeleted);
            runsOk.increment();
            if (totalDeleted > 0) {
                log.info("replay_idempotency GC reclaimed {} row(s) older than {}h in {} ms",
                        totalDeleted, retentionHours, Duration.ofNanos(System.nanoTime() - startNanos).toMillis());
            }
        } catch (DataAccessException e) {
            // Connectivity / timeout / transient DB error. Committed chunks are durable; resume next run.
            runsFailed.increment();
            log.error("replay_idempotency GC aborted after {} row(s) reclaimed: {}",
                    totalDeleted, e.getMessage(), e);
        } finally {
            running.set(false);
        }
    }
}
