package io.chaosforge.controlplane.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.LocalDate;
import java.time.ZoneOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Daily-partition lifecycle for the range-partitioned CP {@code outbox} (acceptance gate C28; mirrors
 * the Execution Service {@code PartitionMaintenance}). Purge is a partition {@code DROP} — O(1)
 * metadata, no dead tuples, no autovacuum churn — replacing the old {@code DELETE … WHERE status='SENT'
 * AND sent_at < cutoff} path. Drop is age-based; the rare aged-yet-unsent row is a DEAD /
 * oldest-pending-age incident already surfaced by the C26/C27 alerts before its partition ages out.
 */
@Component
public class PartitionMaintenance {

    private static final Logger log = LoggerFactory.getLogger(PartitionMaintenance.class);
    private static final String PARENT = "outbox";

    private final JdbcTemplate jdbc;
    private final MeterRegistry meterRegistry;

    // Stall-runway, not retention-bounded — create-ahead/drop-old never overlap (partitioning-rules.md §1).
    @Value("${chaosforge.partition.lookahead-days:4}")
    private int lookaheadDays;

    @Value("${chaosforge.partition.outbox-retention-days:3}")
    private int outboxRetentionDays;

    public PartitionMaintenance(JdbcTemplate jdbc, MeterRegistry meterRegistry) {
        this.jdbc = jdbc;
        this.meterRegistry = meterRegistry;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void ensureOnStartup() {
        registerDefaultOccupancyGauge();
        ensureAhead();
    }

    /**
     * A row in {@code outbox_default} is the one failure this design cannot self-heal: it can never be
     * dropped (the drop matches only {@code _pYYYYMMDD}) and it permanently blocks creating that day's
     * real partition (arch-audit 2.2). Publish a gauge so occupancy {@code > 0} is alertable rather than
     * a silent disk leak. {@code FROM ONLY} counts just the default child, not the whole tree.
     */
    private void registerDefaultOccupancyGauge() {
        Gauge.builder("chaosforge.partition.default_rows", this::defaultRowCount)
                .tag("table", PARENT)
                .description("Rows trapped in the un-droppable DEFAULT partition — must stay 0")
                .register(meterRegistry);
    }

    private double defaultRowCount() {
        Long n = jdbc.queryForObject("SELECT count(*) FROM ONLY " + PARENT + "_default", Long.class);
        return n == null ? 0d : n;
    }

    @Scheduled(cron = "${chaosforge.partition.maintenance-cron:0 15 3 * * *}")
    public void maintain() {
        ensureAhead();
        dropOld();
    }

    /** Create today…today+lookahead daily partitions (idempotent). */
    public void ensureAhead() {
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        for (int d = 0; d <= lookaheadDays; d++) {
            LocalDate day = today.plusDays(d);
            try {
                ensureDailyPartition(day);
            } catch (RuntimeException e) {
                // Create failure likely means a row is trapped in outbox_default for `day`; keep going.
                log.error("failed to ensure outbox partition for {} — rows may be trapped in {}_default",
                        day, PARENT, e);
            }
        }
    }

    /** Drop outbox partitions older than retention. @return partitions dropped. */
    public int dropOld() {
        LocalDate cutoff = LocalDate.now(ZoneOffset.UTC).minusDays(outboxRetentionDays);
        Integer n = jdbc.queryForObject(
                "SELECT cf_drop_partitions_before(?, CAST(? AS date))", Integer.class, PARENT, cutoff.toString());
        int dropped = n == null ? 0 : n;
        if (dropped > 0) {
            meterRegistry.counter("chaosforge.partition.dropped_total", "table", PARENT).increment(dropped);
            log.info("outbox partition purge dropped {} aged partition(s) (>{}d)", dropped, outboxRetentionDays);
        }
        return dropped;
    }

    /** Ensure a single daily partition exists. Exposed for the maintenance IT to seed an aged day. */
    public void ensureDailyPartition(LocalDate day) {
        jdbc.queryForList("SELECT cf_ensure_daily_partition(?, CAST(? AS date))", PARENT, day.toString());
    }
}
