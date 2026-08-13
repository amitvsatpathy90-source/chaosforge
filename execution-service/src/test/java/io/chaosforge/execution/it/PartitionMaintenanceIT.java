package io.chaosforge.execution.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.f4b6a3.uuid.UuidCreator;
import io.chaosforge.execution.partition.PartitionMaintenance;
import io.chaosforge.execution.persistence.InboxDao;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Acceptance gate <b>C28</b> — inbox/outbox purge by partition-drop, against real Postgres. Proves the
 * two things the design rests on: (1) inbox dedup still holds on the range-partitioned table (the
 * UUIDv7-derived {@code msg_ts} keeps a redelivery in the same partition), and (2) purging an aged day
 * is a {@code DROP TABLE} of one partition — O(1) metadata, the rows vanish with the partition (no
 * bulk DELETE, no dead tuples for autovacuum to chase under load), while fresh rows are untouched.
 */
class PartitionMaintenanceIT extends ExecPostgresIT {

    private static final DateTimeFormatter YMD = DateTimeFormatter.BASIC_ISO_DATE;

    private PartitionMaintenance maintenance() {
        PartitionMaintenance m = new PartitionMaintenance(jdbc, new SimpleMeterRegistry());
        ReflectionTestUtils.setField(m, "lookaheadDays", 2);
        ReflectionTestUtils.setField(m, "inboxRetentionDays", 7);
        ReflectionTestUtils.setField(m, "outboxRetentionDays", 3);
        return m;
    }

    @Test
    void ensureAhead_createsTodayAndLookahead_forBothTables() {
        maintenance().ensureAhead();

        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        for (String parent : List.of("inbox", "outbox")) {
            for (int d = 0; d <= 2; d++) {
                assertThat(partitionExists(parent, today.plusDays(d)))
                        .as("%s partition for today+%d must exist", parent, d).isTrue();
            }
        }
    }

    @Test
    void inboxDedup_holdsAcrossRedelivery_onThePartitionedTable() {
        maintenance().ensureAhead();                 // today's partition for the message's embedded time
        InboxDao dao = new InboxDao(jdbc);
        UUID msg = UuidCreator.getTimeOrderedEpoch();
        UUID tenant = UUID.randomUUID();
        UUID scenario = UUID.randomUUID();

        assertThat(dao.insertIfAbsent(msg, tenant, scenario)).as("first sight").isTrue();
        assertThat(dao.insertIfAbsent(msg, tenant, scenario)).as("redelivery is deduped").isFalse();
        assertThat(rowCount("inbox")).as("exactly one effect despite redelivery").isEqualTo(1L);
    }

    @Test
    void partitionDrop_purgesAnAgedInboxDay_andLeavesFreshRows() {
        PartitionMaintenance m = maintenance();
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        LocalDate agedDay = today.minusDays(30);     // well past the 7-day inbox retention

        m.ensureDailyPartition("inbox", agedDay);
        m.ensureAhead();                             // today's partition for the fresh row

        // 500 aged rows routed into the aged day-partition, plus one fresh row today.
        Instant agedTs = agedDay.atStartOfDay(ZoneOffset.UTC).plusHours(12).toInstant();
        for (int i = 0; i < 500; i++) {
            insertInbox(UuidCreator.getTimeOrderedEpoch(), agedTs);
        }
        UUID freshMsg = UuidCreator.getTimeOrderedEpoch();
        insertInbox(freshMsg, Instant.now());

        assertThat(partitionExists("inbox", agedDay)).isTrue();
        assertThat(partitionSizeBytes("inbox", agedDay)).as("aged partition holds data").isGreaterThan(0L);
        assertThat(rowCount("inbox")).isEqualTo(501L);

        int dropped = m.dropOld();

        assertThat(dropped).as("at least the aged inbox day was dropped").isGreaterThanOrEqualTo(1);
        assertThat(partitionExists("inbox", agedDay))
                .as("aged partition gone — purge is an O(1) DROP, not a row-by-row DELETE").isFalse();
        assertThat(rowCount("inbox")).as("aged rows purged with the partition; fresh row survives").isEqualTo(1L);
        assertThat(inboxContains(freshMsg)).as("today's row is untouched").isTrue();
    }

    @Test
    void partitionDrop_purgesAnAgedOutboxDay() {
        PartitionMaintenance m = maintenance();
        LocalDate agedDay = LocalDate.now(ZoneOffset.UTC).minusDays(10);   // past the 3-day outbox retention
        m.ensureDailyPartition("outbox", agedDay);

        Instant agedTs = agedDay.atStartOfDay(ZoneOffset.UTC).plusHours(6).toInstant();
        jdbc.update("INSERT INTO outbox (message_id, msg_ts, aggregate_id, tenant_id, topic, partition_key, "
                        + "replay_version, payload, status, sent_at) "
                        + "VALUES (?, ?, ?, ?, 'chaosforge.scenario.results.v1', ?, 1, ?, 'SENT', now())",
                UuidCreator.getTimeOrderedEpoch(), Timestamp.from(agedTs), UUID.randomUUID(), UUID.randomUUID(),
                UUID.randomUUID().toString(), new byte[] {0x1});

        assertThat(partitionExists("outbox", agedDay)).isTrue();
        m.dropOld();
        assertThat(partitionExists("outbox", agedDay)).as("aged outbox day dropped by retention").isFalse();
    }

    // ---- helpers --------------------------------------------------------------------------------

    private void insertInbox(UUID messageId, Instant msgTs) {
        jdbc.update("INSERT INTO inbox (message_id, msg_ts, tenant_id, scenario_id) VALUES (?, ?, ?, ?)",
                messageId, Timestamp.from(msgTs), UUID.randomUUID(), UUID.randomUUID());
    }

    private boolean partitionExists(String parent, LocalDate day) {
        Boolean present = jdbc.queryForObject(
                "SELECT to_regclass(?) IS NOT NULL", Boolean.class, parent + "_p" + day.format(YMD));
        return Boolean.TRUE.equals(present);
    }

    private long partitionSizeBytes(String parent, LocalDate day) {
        Long size = jdbc.queryForObject(
                "SELECT pg_total_relation_size(?)", Long.class, parent + "_p" + day.format(YMD));
        return size == null ? 0L : size;
    }

    private long rowCount(String table) {
        Long c = jdbc.queryForObject("SELECT count(*) FROM " + table, Long.class);
        return c == null ? -1L : c;
    }

    private boolean inboxContains(UUID messageId) {
        Integer c = jdbc.queryForObject("SELECT count(*) FROM inbox WHERE message_id = ?", Integer.class, messageId);
        return c != null && c == 1;
    }
}
