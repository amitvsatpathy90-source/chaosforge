package io.chaosforge.controlplane.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.chaosforge.controlplane.janitor.ReplayIdempotencyGcDao;
import io.chaosforge.controlplane.janitor.ReplayIdempotencyJanitor;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * ADR-0528 replay_idempotency TTL janitor against real Postgres (no Spring context — drives the DAO +
 * scheduled bean directly). Verifies the chunked, index-backed delete drains the expired set across
 * multiple short transactions, retains in-window rows, guards against overlapping runs, and records
 * the right metrics.
 */
class ReplayIdempotencyJanitorIT extends CpPostgresIT {

    private static final String DELETED = "chaosforge.replay_idempotency.gc.deleted";
    private static final String RUNS = "chaosforge.replay_idempotency.gc.runs";

    @Test
    void chunkedDelete_reclaimsExpired_retainsInWindow() {
        // 5 COMPLETED + 2 abandoned IN_PROGRESS past the 24h window (all expired) + 3 fresh.
        for (int i = 0; i < 5; i++) insert(48, "COMPLETED");
        for (int i = 0; i < 2; i++) insert(48, "IN_PROGRESS");
        for (int i = 0; i < 3; i++) insert(1, "COMPLETED");
        assertThat(rowCount()).isEqualTo(10);

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ReplayIdempotencyJanitor janitor = janitor(registry, /*chunkSize*/ 2);   // force ≥4 chunks over 7

        janitor.purgeExpired();

        assertThat(rowCount()).as("only in-window rows survive").isEqualTo(3);
        assertThat(registry.get(DELETED).counter().count()).isEqualTo(7.0);
        assertThat(registry.get(RUNS).tag("outcome", "success").counter().count()).isEqualTo(1.0);
    }

    @Test
    void overlapGuard_skipsWhenPreviousRunInFlight() {
        insert(48, "COMPLETED");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ReplayIdempotencyJanitor janitor = janitor(registry, 500);
        // Simulate a previous run still in flight.
        ((AtomicBoolean) ReflectionTestUtils.getField(janitor, "running")).set(true);

        janitor.purgeExpired();

        assertThat(rowCount()).as("a guarded run deletes nothing").isEqualTo(1);
        assertThat(registry.get(RUNS).tag("outcome", "skipped").counter().count()).isEqualTo(1.0);
    }

    @Test
    void emptyExpiredSet_isNoOp_successCounted() {
        insert(1, "COMPLETED");   // in-window only
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ReplayIdempotencyJanitor janitor = janitor(registry, 500);

        janitor.purgeExpired();

        assertThat(rowCount()).isEqualTo(1);
        assertThat(registry.get(DELETED).counter().count()).isEqualTo(0.0);
        assertThat(registry.get(RUNS).tag("outcome", "success").counter().count()).isEqualTo(1.0);
    }

    private ReplayIdempotencyJanitor janitor(SimpleMeterRegistry registry, int chunkSize) {
        ReplayIdempotencyJanitor janitor = new ReplayIdempotencyJanitor(new ReplayIdempotencyGcDao(jdbc), registry);
        ReflectionTestUtils.setField(janitor, "retentionHours", 24);
        ReflectionTestUtils.setField(janitor, "chunkSize", chunkSize);
        ReflectionTestUtils.setField(janitor, "maxChunksPerRun", 1000);
        return janitor;
    }

    private void insert(int ageHours, String status) {
        jdbc.update("INSERT INTO replay_idempotency (tenant_id, idempotency_key, status, created_at) "
                        + "VALUES (?, ?, ?, now() - make_interval(hours => ?))",
                UUID.randomUUID(), UUID.randomUUID(), status, ageHours);
    }

    private int rowCount() {
        Integer c = jdbc.queryForObject("SELECT count(*) FROM replay_idempotency", Integer.class);
        return c == null ? -1 : c;
    }
}
