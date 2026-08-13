package io.chaosforge.execution.it;

import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Base for Execution Service integration tests: one shared Postgres container + the real exec Flyway
 * migrations (V4–V10). Drives the real claim/inbox/fence DAOs against real Postgres row locking.
 */
abstract class ExecPostgresIT {

    @SuppressWarnings("resource")
    private static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16.4-alpine");

    protected static final JdbcTemplate jdbc;
    protected static final TransactionTemplate tx;
    protected static final DataSource ds;

    static {
        PG.start();
        Flyway.configure()
                .dataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword())
                .locations("classpath:db/migration")
                .load()
                .migrate();
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                PG.getJdbcUrl(), PG.getUsername(), PG.getPassword());
        dataSource.setDriverClassName("org.postgresql.Driver");
        ds = dataSource;
        jdbc = new JdbcTemplate(ds);
        tx = new TransactionTemplate(new DataSourceTransactionManager(ds));
    }

    @BeforeEach
    void cleanTables() {
        jdbc.execute("TRUNCATE inbox, inbox_fence, scenario_run, scenario_run_log, outbox CASCADE");
    }
}
