package io.chaosforge.execution.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Diagnostic/regression: boots the real Execution Service context against a FRESH Postgres and asserts
 * the Flyway migrations (V4–V7) actually ran. The DAOs are lazy JdbcTemplate calls, so a context that
 * never migrated still "loads" — this test fails loudly instead, the way a production startup must.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        properties = {
                "spring.kafka.bootstrap-servers=localhost:9092",
                "spring.kafka.listener.auto-startup=false"   // no broker here — Flyway is what we test
        })
@Testcontainers
class FlywaySchemaMigrationIT {

    @Container
    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16.4-alpine");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", PG::getJdbcUrl);
        registry.add("spring.datasource.username", PG::getUsername);
        registry.add("spring.datasource.password", PG::getPassword);
        registry.add("spring.security.oauth2.resourceserver.jwt.jwk-set-uri",
                () -> "http://localhost:0/.well-known/jwks.json");
    }

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private MeterRegistry meterRegistry;

    @Test
    void execSchemaIsMigratedAtStartup() {
        for (String table : new String[] {"outbox", "inbox", "inbox_fence", "scenario_run", "scenario_run_log"}) {
            Integer present = jdbc.queryForObject(
                    "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name=?",
                    Integer.class, table);
            assertThat(present).as("table %s must exist after Flyway migration", table).isEqualTo(1);
        }
        Integer applied = jdbc.queryForObject(
                "SELECT count(*) FROM flyway_schema_history WHERE success = true", Integer.class);
        assertThat(applied).as("Flyway must have applied the V4–V7 migrations").isGreaterThanOrEqualTo(4);
    }

    @Test
    void schemaStateIsObservable() {
        // Snapshotted at ApplicationReadyEvent, which fires before tests run.
        assertThat(gauge("db.schema.version"))
                .as("latest applied version is exposed").isGreaterThanOrEqualTo(7.0);
        assertThat(gauge("db.schema.migrations.applied"))
                .as("applied count is exposed").isGreaterThanOrEqualTo(4.0);
        assertThat(gauge("db.schema.migrations.pending"))
                .as("a healthy startup leaves zero pending migrations").isZero();
    }

    private double gauge(String name) {
        return meterRegistry.get(name).gauge().value();
    }
}
