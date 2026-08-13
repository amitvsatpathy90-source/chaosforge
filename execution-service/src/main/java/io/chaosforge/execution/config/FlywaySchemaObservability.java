package io.chaosforge.execution.config;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.util.concurrent.atomic.AtomicLong;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationInfo;
import org.flywaydb.core.api.MigrationInfoService;
import org.flywaydb.core.api.MigrationVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

/**
 * Schema-migration observability. Boot 4 runs Flyway at startup and <b>fails fast</b> on a bad
 * migration (the service never starts serving on a broken schema — see {@code spring-boot-starter-flyway}
 * in the build). This adds the <em>visible</em> half: Prometheus gauges for the live schema state and
 * one authoritative log line at startup.
 *
 * <p>The schema only changes at startup, so the values are snapshotted once on {@link ApplicationReadyEvent}
 * (no per-scrape DB hit on {@code flyway_schema_history}). All gauges are low-cardinality (no
 * {@code tenant_id}). A non-zero {@code db.schema.migrations.pending} is the alert signal — a replica
 * is running behind its migrations.
 */
@Configuration
public class FlywaySchemaObservability {

    private static final Logger log = LoggerFactory.getLogger(FlywaySchemaObservability.class);

    private final AtomicLong schemaVersion = new AtomicLong(-1);
    private final AtomicLong appliedCount = new AtomicLong(-1);
    private final AtomicLong pendingCount = new AtomicLong(-1);

    @Bean
    public MeterBinder flywaySchemaMeters() {
        return registry -> {
            Gauge.builder("db.schema.version", schemaVersion, AtomicLong::doubleValue)
                    .description("Latest applied Flyway schema version as a number (-1 if none/non-numeric)")
                    .register(registry);
            Gauge.builder("db.schema.migrations.applied", appliedCount, AtomicLong::doubleValue)
                    .description("Count of successfully applied Flyway migrations")
                    .register(registry);
            Gauge.builder("db.schema.migrations.pending", pendingCount, AtomicLong::doubleValue)
                    .description("Count of pending Flyway migrations; non-zero means the schema is behind")
                    .register(registry);
        };
    }

    @EventListener(ApplicationReadyEvent.class)
    public void captureSchemaState(ApplicationReadyEvent event) {
        Flyway flyway = event.getApplicationContext().getBeanProvider(Flyway.class).getIfAvailable();
        if (flyway == null) {
            log.warn("Flyway is not configured — this service is NOT managing its own schema");
            return;
        }
        MigrationInfoService info = flyway.info();
        MigrationInfo current = info.current();
        appliedCount.set(info.applied().length);
        pendingCount.set(info.pending().length);
        schemaVersion.set(numericVersion(current));
        log.info("schema migration ready: current_version={} applied={} pending={}",
                current == null ? "none" : current.getVersion(), info.applied().length, info.pending().length);
    }

    /** Our migrations are integer-versioned (V4…V7). A non-numeric/absent version maps to -1. */
    private static long numericVersion(MigrationInfo current) {
        if (current == null) {
            return -1L;
        }
        MigrationVersion version = current.getVersion();
        if (version == null) {
            return -1L;
        }
        try {
            return Long.parseLong(version.getVersion());
        } catch (NumberFormatException e) {
            return -1L;
        }
    }
}
