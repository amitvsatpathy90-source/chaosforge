package io.chaosforge.controlplane.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Regression guard: the Control Plane must migrate its own schema (V1–V8) at startup via Boot's Flyway
 * autoconfiguration — not via a test that runs Flyway by hand. {@link ControlPlaneContextIT} only checks
 * bean wiring, so it stayed green even when Flyway silently never ran (Boot 4 moved the Flyway autoconfig
 * to the {@code spring-boot-flyway} module; the build now pulls it via {@code spring-boot-starter-flyway}).
 * This asserts the actual schema + the observability gauges.
 */
class CpFlywaySchemaMigrationIT extends AbstractCpIntegrationTest {

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private MeterRegistry meterRegistry;

    @Test
    void cpSchemaIsMigratedAtStartup() {
        for (String table : new String[] {"tenants", "scenarios", "outbox", "replay_idempotency"}) {
            Integer present = jdbc.queryForObject(
                    "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name=?",
                    Integer.class, table);
            assertThat(present).as("table %s must exist after Flyway migration", table).isEqualTo(1);
        }
        Integer applied = jdbc.queryForObject(
                "SELECT count(*) FROM flyway_schema_history WHERE success = true", Integer.class);
        assertThat(applied).as("Flyway must have applied the CP migrations (V1–V8)").isGreaterThanOrEqualTo(5);
    }

    @Test
    void schemaStateIsObservable() {
        assertThat(meterRegistry.get("db.schema.version").gauge().value())
                .as("latest applied version is exposed").isGreaterThanOrEqualTo(8.0);
        assertThat(meterRegistry.get("db.schema.migrations.pending").gauge().value())
                .as("a healthy startup leaves zero pending migrations").isZero();
    }
}
