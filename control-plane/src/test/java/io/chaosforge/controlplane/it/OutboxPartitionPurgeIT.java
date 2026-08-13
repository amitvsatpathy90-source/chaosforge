package io.chaosforge.controlplane.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.f4b6a3.uuid.UuidCreator;
import io.chaosforge.controlplane.outbox.PartitionMaintenance;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Acceptance gate <b>C28</b> (CP outbox half) — the range-partitioned CP {@code outbox} is purged by
 * dropping an aged day-partition, against real Postgres. Mirrors the Execution Service
 * {@code PartitionMaintenanceIT}: an O(1) {@code DROP} of one partition replaces a status-scoped bulk
 * DELETE, leaving no dead tuples for autovacuum, while fresh rows are untouched.
 */
class OutboxPartitionPurgeIT extends CpPostgresIT {

    private static final DateTimeFormatter YMD = DateTimeFormatter.BASIC_ISO_DATE;

    private PartitionMaintenance maintenance() {
        PartitionMaintenance m = new PartitionMaintenance(jdbc, new SimpleMeterRegistry());
        ReflectionTestUtils.setField(m, "lookaheadDays", 2);
        ReflectionTestUtils.setField(m, "outboxRetentionDays", 3);
        return m;
    }

    @Test
    void ensureAhead_createsTodayAndLookaheadOutboxPartitions() {
        maintenance().ensureAhead();
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        for (int d = 0; d <= 2; d++) {
            assertThat(partitionExists(today.plusDays(d))).as("outbox partition today+%d", d).isTrue();
        }
    }

    @Test
    void partitionDrop_purgesAnAgedOutboxDay_andLeavesFreshRows() {
        PartitionMaintenance m = maintenance();
        LocalDate agedDay = LocalDate.now(ZoneOffset.UTC).minusDays(10);   // past the 3-day retention
        m.ensureDailyPartition(agedDay);
        m.ensureAhead();

        Instant agedTs = agedDay.atStartOfDay(ZoneOffset.UTC).plusHours(6).toInstant();
        insertOutbox(agedTs, "SENT");                 // an aged, delivered row
        insertOutbox(Instant.now(), "PENDING");       // a fresh row today

        assertThat(partitionExists(agedDay)).isTrue();
        assertThat(rowCount()).isEqualTo(2L);

        int dropped = m.dropOld();

        assertThat(dropped).as("the aged outbox day was dropped").isGreaterThanOrEqualTo(1);
        assertThat(partitionExists(agedDay)).as("aged partition gone — O(1) DROP, no DELETE/vacuum").isFalse();
        assertThat(rowCount()).as("aged row purged with the partition; fresh row survives").isEqualTo(1L);
    }

    private void insertOutbox(Instant msgTs, String status) {
        jdbc.update("INSERT INTO outbox (message_id, msg_ts, aggregate_id, tenant_id, topic, partition_key, "
                        + "replay_version, rule_set_id, rule_set_version, payload, status) "
                        + "VALUES (?, ?, ?, ?, 'chaosforge.scenario.commands.v1', ?, 1, ?, 1, ?, ?)",
                UuidCreator.getTimeOrderedEpoch(), Timestamp.from(msgTs), UUID.randomUUID(), UUID.randomUUID(),
                UUID.randomUUID().toString(), UUID.randomUUID(), new byte[] {0x1}, status);
    }

    private boolean partitionExists(LocalDate day) {
        Boolean present = jdbc.queryForObject(
                "SELECT to_regclass(?) IS NOT NULL", Boolean.class, "outbox_p" + day.format(YMD));
        return Boolean.TRUE.equals(present);
    }

    private long rowCount() {
        Long c = jdbc.queryForObject("SELECT count(*) FROM outbox", Long.class);
        return c == null ? -1L : c;
    }
}
