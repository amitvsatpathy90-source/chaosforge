package io.chaosforge.execution.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.chaosforge.execution.control.KillSwitch;
import io.chaosforge.execution.persistence.RunLogDao;
import io.chaosforge.execution.pipeline.ExecutePhase;
import io.chaosforge.execution.pipeline.ExecutionResult;
import io.chaosforge.execution.ruleset.CpRuleSetLoader;
import io.chaosforge.execution.ruleset.RuleSetRef;
import io.chaosforge.execution.ruleset.StepSpec;
import io.chaosforge.execution.steadystate.SteadyStateGuard;
import io.chaosforge.execution.step.StepExecutor;
import io.chaosforge.schema.v1.ScenarioRunCommand;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Acceptance gate <b>C19</b> — the executor's stop-controls hold under real infrastructure faults,
 * driven by pausing / stopping a dedicated Postgres container (its own, never the shared suite
 * singleton, since these tests destroy it).
 *
 * <ol>
 *   <li><b>Network partition</b> (container <i>paused</i> — packets dropped, reads hang): a DB read on
 *       an established connection aborts at the configured {@code socketTimeout} instead of hanging
 *       forever, so a hung statement can never stall a consumer thread past {@code max.poll.interval.ms}
 *       (this is what {@code application.yml}'s {@code socketTimeout}/{@code statement_timeout} buy).</li>
 *   <li><b>Postgres down</b> (container <i>stopped</i>): the in-memory {@link KillSwitch} still aborts an
 *       in-flight scenario — the emergency stop is reachable during exactly the outage it exists for,
 *       and it issues zero fault steps.</li>
 * </ol>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ExecutorControlsUnderFaultIT {

    @SuppressWarnings("resource")
    private static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16.4-alpine");

    @BeforeAll
    static void start() {
        PG.start();   // no Flyway needed — these tests never touch application tables
    }

    @AfterAll
    static void stop() {
        unpauseQuietly();
        if (PG.isRunning()) {
            PG.stop();
        }
    }

    @Test
    @Order(1)
    void databaseRead_underNetworkPartition_abortsAtSocketTimeout_notAnInfiniteHang() throws Exception {
        // socketTimeout=2s as a URL param (PgJDBC is runtimeOnly — no compile-time import). Production
        // sets 30s in application.yml; the test uses 2s so a partitioned read aborts fast and provably.
        String url = String.format("jdbc:postgresql://%s:%d/%s?socketTimeout=2&connectTimeout=3",
                PG.getHost(), PG.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT), PG.getDatabaseName());
        DriverManagerDataSource ds = new DriverManagerDataSource(url, PG.getUsername(), PG.getPassword());
        ds.setDriverClassName("org.postgresql.Driver");

        try (Connection conn = ds.getConnection()) {
            try (Statement warm = conn.createStatement()) {
                warm.execute("SELECT 1");   // establish the connection while the broker is reachable
            }

            pause();   // freeze the container → an in-flight read gets no response (a partition)

            long startMs = System.currentTimeMillis();
            assertThatThrownBy(() -> {
                try (Statement st = conn.createStatement()) {
                    st.execute("SELECT 1");
                }
            }).isInstanceOf(SQLException.class);
            long elapsedMs = System.currentTimeMillis() - startMs;

            assertThat(elapsedMs)
                    .as("a partitioned DB read aborts at ~socketTimeout (2s), never an unbounded hang")
                    .isLessThan(8_000L);
        } finally {
            unpauseQuietly();
        }
    }

    @Test
    @Order(2)
    void jdbcTemplateRead_underNetworkPartition_throwsDataAccessException() throws Exception {
        // CHAOS-01 grounding: the pipeline DAOs use JdbcTemplate, which translates a driver SQLException
        // into a Spring DataAccessException — the family ScenarioCommandListener maps to INFRA_TRANSIENT.
        // Order(1) proves the raw read aborts bounded; this proves the TRANSLATED type, so a partitioned DB
        // read can never escape to dlqReasonFor's SCHEMA_INVALID fail-closed default. A SingleConnectionDataSource
        // reuses one physical connection so the paused read is a socket-read timeout mid-query (SQLState 08) —
        // the exact case the resilience audit flagged as a NonTransient DataAccessResourceFailureException.
        String url = String.format("jdbc:postgresql://%s:%d/%s?socketTimeout=2&connectTimeout=3",
                PG.getHost(), PG.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT), PG.getDatabaseName());
        SingleConnectionDataSource ds =
                new SingleConnectionDataSource(url, PG.getUsername(), PG.getPassword(), true);
        ds.setDriverClassName("org.postgresql.Driver");
        JdbcTemplate jdbc = new JdbcTemplate(ds);
        jdbc.execute("SELECT 1");   // establish the single connection while the broker is reachable

        pause();   // freeze the container → the in-flight read on that connection gets no response
        try {
            assertThatThrownBy(() -> jdbc.queryForObject("SELECT 1", Integer.class))
                    .as("JdbcTemplate translates the partitioned read to a Spring DataAccessException — the "
                            + "family the listener maps to INFRA_TRANSIENT, never a raw SQLException")
                    .isInstanceOf(DataAccessException.class);
        } finally {
            unpauseQuietly();
            try {
                ds.destroy();
            } catch (Exception ignored) {
                // the partition may have already broken the physical connection — nothing to clean up
            }
        }
    }

    @Test
    @Order(3)
    void killSwitch_abortsExecution_withPostgresStopped() {
        // Rule sets load from the CP over HTTP + cache (not the exec DB), so a 2-step rule set is
        // available even with Postgres down; the StepExecutor fails the test if a single step runs.
        CpRuleSetLoader ruleSetLoader = mock(CpRuleSetLoader.class);
        when(ruleSetLoader.loadPinned(any(), anyInt(), any()))
                .thenReturn(new RuleSetRef(UUID.randomUUID(), 1, List.of(step("s1"), step("s2"))));
        StepExecutor stepExecutor = mock(StepExecutor.class);
        when(stepExecutor.execute(any(), anyLong(), any()))
                .thenThrow(new AssertionError("no fault step may run while the kill switch is engaged"));

        DriverManagerDataSource deadDs = new DriverManagerDataSource(
                PG.getJdbcUrl(), PG.getUsername(), PG.getPassword());
        deadDs.setDriverClassName("org.postgresql.Driver");
        RunLogDao runLogDao = new RunLogDao(new JdbcTemplate(deadDs));   // points at the about-to-die DB
        KillSwitch killSwitch = new KillSwitch(new SimpleMeterRegistry());
        SteadyStateGuard steadyState = new SteadyStateGuard(url -> true, new SimpleMeterRegistry());  // disabled
        ExecutePhase executePhase = new ExecutePhase(
                ruleSetLoader, stepExecutor, runLogDao, killSwitch, steadyState, 240_000);

        killSwitch.engage("incident: halt all fault injection");
        PG.stop();   // Postgres is now DOWN

        ExecutionResult result = executePhase.execute(command(), 5L);

        assertThat(result.outcome())
                .as("the in-memory kill switch aborts the run without any DB access")
                .isEqualTo("ABORTED");
        verify(stepExecutor, never()).execute(any(), anyLong(), any());
    }

    private static void pause() {
        PG.getDockerClient().pauseContainerCmd(PG.getContainerId()).exec();
    }

    private static void unpauseQuietly() {
        try {
            if (PG.isRunning()) {
                PG.getDockerClient().unpauseContainerCmd(PG.getContainerId()).exec();
            }
        } catch (RuntimeException ignored) {
            // already unpaused / stopped — nothing to undo
        }
    }

    private static StepSpec step(String id) {
        return new StepSpec(id, "LATENCY", "http://target/" + id, "GET", Map.of());
    }

    private static ScenarioRunCommand command() {
        return ScenarioRunCommand.newBuilder()
                .setScenarioId(UUID.randomUUID().toString())
                .setTenantId(UUID.randomUUID().toString())
                .setReplayVersion(5L)
                .setRuleSetId(UUID.randomUUID().toString())
                .setRuleSetVersion(1)
                .setSteps(List.of())
                .setCommandIssuedAt(Instant.now())
                .build();
    }
}
