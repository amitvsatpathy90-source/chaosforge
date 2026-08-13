package io.chaosforge.controlplane.it;

import io.chaosforge.controlplane.replay.CommandPayloadCodec;
import io.chaosforge.controlplane.replay.ScenarioReplayOrchestrator;
import io.chaosforge.controlplane.repository.OutboxRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Base for Control Plane integration tests: one shared Postgres container (singleton pattern — started
 * once, reaped at JVM exit), the real Flyway migrations (V1–V11) applied, and the <b>real</b>
 * {@link ScenarioReplayOrchestrator} driven through a {@link TransactionTemplate} so its
 * {@code @Transactional} 5-statement critical section runs in one transaction (ADR-0528) against real
 * Postgres row locking — no Spring context, no mocks of the DB.
 */
abstract class CpPostgresIT {

    @SuppressWarnings("resource")
    private static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16.4-alpine");

    protected static final DriverManagerDataSource ds;
    protected static final JdbcTemplate jdbc;
    protected static final TransactionTemplate tx;

    static {
        PG.start();   // singleton; Testcontainers Ryuk stops it at JVM exit
        Flyway.configure()
                .dataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword())
                .locations("classpath:db/migration")
                .load()
                .migrate();
        ds = new DriverManagerDataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword());
        ds.setDriverClassName("org.postgresql.Driver");
        jdbc = new JdbcTemplate(ds);
        tx = new TransactionTemplate(new DataSourceTransactionManager(ds));
    }

    @BeforeEach
    void cleanTables() {
        jdbc.execute("TRUNCATE replay_idempotency, outbox, scenario_replay_state, scenarios, "
                + "rule_sets, tenants CASCADE");
    }

    /** Seeds tenant → rule_set(version) → scenario; the scenarios INSERT trigger seeds replay_state=0. */
    protected void seed(UUID tenant, UUID ruleSet, int version, UUID scenario) {
        jdbc.update("INSERT INTO tenants (tenant_id, name, rate_limit_per_min) VALUES (?, ?, ?)",
                tenant, "tenant-" + tenant.toString().substring(0, 8), 600);
        jdbc.update("INSERT INTO rule_sets (rule_set_id, version, tenant_id, name, definition) "
                + "VALUES (?, ?, ?, ?, CAST(? AS jsonb))", ruleSet, version, tenant, "rs", "{}");
        jdbc.update("INSERT INTO scenarios (scenario_id, tenant_id, name, rule_set_id, rule_set_version) "
                + "VALUES (?, ?, ?, ?, ?)", scenario, tenant, "sc", ruleSet, version);
    }

    protected long replayVersionOf(UUID scenario) {
        Long v = jdbc.queryForObject(
                "SELECT replay_version FROM scenario_replay_state WHERE scenario_id = ?", Long.class, scenario);
        return v == null ? -1L : v;
    }

    /** The real orchestrator, wired with a JdbcTemplate-backed outbox insert and a stub Avro codec. */
    protected ScenarioReplayOrchestrator newOrchestrator() {
        return new ScenarioReplayOrchestrator(jdbc, jdbcOutboxRepository(), stubCodec(), new SimpleMeterRegistry());
    }

    private OutboxRepository jdbcOutboxRepository() {
        return (messageId, aggregateId, tenantId, topic, partitionKey, replayVersion, ruleSetId,
                ruleSetVersion, payload, tenantSignature, headers) ->
                jdbc.update("INSERT INTO outbox (message_id, aggregate_id, tenant_id, topic, partition_key, "
                                + "replay_version, rule_set_id, rule_set_version, payload, tenant_signature, headers) "
                                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb))",
                        messageId, aggregateId, tenantId, topic, partitionKey, replayVersion, ruleSetId,
                        ruleSetVersion, payload, tenantSignature, headers);
    }

    private CommandPayloadCodec stubCodec() {
        // The critical-section tests assert on tokens/versions/exceptions, not payload bytes.
        return payload -> new byte[] {0x1, 0x2, 0x3};
    }
}
