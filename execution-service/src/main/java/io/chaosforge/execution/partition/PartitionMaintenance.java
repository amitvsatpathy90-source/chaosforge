package io.chaosforge.execution.partition;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Daily-partition lifecycle for range-partitioned {@code inbox}/{@code outbox} (C28). Purge is
 * partition DROP (O(1), no autovacuum churn), replacing the old DELETE-by-cutoff path.
 * Create-ahead ensures today+lookahead partitions exist before rows need them; drop-old removes
 * partitions past retention (inbox 7d dedup window, outbox shorter). Drop is age-based, not
 * status-aware — a stuck old row is already surfaced by the C26/C27 alerts before its partition drops.
 */
@Component
public class PartitionMaintenance {

    private static final Logger log = LoggerFactory.getLogger(PartitionMaintenance.class);
    private static final List<String> PARENTS = List.of("inbox", "outbox");

    private final JdbcTemplate jdbc;
    private final MeterRegistry meterRegistry;

    // Stall-runway, not retention-bounded — mint-clock accept window matches partition coverage by
    // construction (CommandDecoder.verifyMintClock; partitioning-rules.md §1).
    @Value("${chaosforge.partition.lookahead-days:4}")
    private int lookaheadDays;

    @Value("${chaosforge.partition.inbox-retention-days:7}")
    private int inboxRetentionDays;

    @Value("${chaosforge.partition.outbox-retention-days:3}")
    private int outboxRetentionDays;

    public PartitionMaintenance(JdbcTemplate jdbc, MeterRegistry meterRegistry) {
        this.jdbc = jdbc;
        this.meterRegistry = meterRegistry;
    }

    /** Partitions must exist before the listener processes traffic — run once the context is ready. */
    @EventListener(ApplicationReadyEvent.class)
    public void ensureOnStartup() {
        registerDefaultOccupancyGauges();
        ensureAhead();
    }

    /**
     * A row in a {@code _default} partition is the one failure this design cannot self-heal: it can
     * never be dropped (the drop matches only {@code _pYYYYMMDD}) and it permanently blocks creating
     * that day's real partition, so all of that day's rows cascade into the default (arch-audit 2.2).
     * Publish a gauge per parent so occupancy {@code > 0} is an alertable signal rather than a silent
     * disk leak. {@code FROM ONLY} counts just the default child, not the whole partition tree.
     */
    private void registerDefaultOccupancyGauges() {
        for (String parent : PARENTS) {
            Gauge.builder("chaosforge.partition.default_rows", () -> defaultRowCount(parent))
                    .tag("table", parent)
                    .description("Rows trapped in the un-droppable DEFAULT partition — must stay 0")
                    .register(meterRegistry);
        }
    }

    private double defaultRowCount(String parent) {
        // parent ∈ PARENTS (a fixed literal list) — no injection surface.
        Long n = jdbc.queryForObject("SELECT count(*) FROM ONLY " + parent + "_default", Long.class);
        return n == null ? 0d : n;
    }

    @Scheduled(cron = "${chaosforge.partition.maintenance-cron:0 15 3 * * *}")
    public void maintain() {
        ensureAhead();
        dropOld();
    }

    /** Create today…today+lookahead daily partitions for every partitioned parent (idempotent). */
    public void ensureAhead() {
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        for (String parent : PARENTS) {
            for (int d = 0; d <= lookaheadDays; d++) {
                LocalDate day = today.plusDays(d);
                try {
                    ensureDailyPartition(parent, day);
                } catch (RuntimeException e) {
                    // Create failure likely means a row is trapped in the DEFAULT partition; keep going.
                    log.error("failed to ensure {} partition for {} — rows may be trapped in {}_default",
                            parent, day, parent, e);
                }
            }
        }
    }

    /** Drop partitions older than each table's retention. @return total partitions dropped. */
    public int dropOld() {
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        int dropped = dropBefore("inbox", today.minusDays(inboxRetentionDays))
                + dropBefore("outbox", today.minusDays(outboxRetentionDays));
        if (dropped > 0) {
            log.info("partition purge dropped {} aged partition(s) (inbox>{}d, outbox>{}d)",
                    dropped, inboxRetentionDays, outboxRetentionDays);
        }
        return dropped;
    }

    /** Ensure a single daily partition exists. Exposed for the maintenance IT to seed an old day. */
    public void ensureDailyPartition(String parent, LocalDate day) {
        // queryForList (not update) — cf_ensure_daily_partition is a SELECT-able void function.
        jdbc.queryForList("SELECT cf_ensure_daily_partition(?, CAST(? AS date))", parent, day.toString());
    }

    private int dropBefore(String parent, LocalDate cutoff) {
        Integer n = jdbc.queryForObject(
                "SELECT cf_drop_partitions_before(?, CAST(? AS date))", Integer.class, parent, cutoff.toString());
        int dropped = n == null ? 0 : n;
        if (dropped > 0) {
            meterRegistry.counter("chaosforge.partition.dropped_total", "table", parent).increment(dropped);
        }
        return dropped;
    }
}
