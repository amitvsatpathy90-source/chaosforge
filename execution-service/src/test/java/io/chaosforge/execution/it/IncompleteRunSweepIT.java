package io.chaosforge.execution.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.chaosforge.execution.observability.ExecutionMetrics;
import io.chaosforge.execution.persistence.ExecOutboxDao;
import io.chaosforge.execution.persistence.RunLogDao;
import io.chaosforge.execution.pipeline.AvroResultPayloadCodec;
import io.chaosforge.execution.pipeline.FinalizePhase;
import io.chaosforge.execution.sweep.IncompleteRunSweeper;
import io.chaosforge.schema.v1.ScenarioRunResult;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.UUID;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * INCOMPLETE-run sweep (Defect A) against real Postgres: a run left {@code IN_PROGRESS} past the
 * Phase-2->3 crash threshold is marked {@code INCOMPLETE}, counted, <b>and emits a terminal
 * {@code INCOMPLETE} result event via the outbox in the same transaction</b> — a swept run reaches a
 * terminal <em>event</em>, not just a terminal state, so a downstream results consumer sees the same
 * contract as a normally finalized run. An in-flight run within its poll budget, and any
 * already-terminal run, are left untouched (and emit nothing). This is the safety net ADR-0523's
 * correctness argument depends on (and the V6 {@code idx_scenario_run_open} index was built for).
 */
class IncompleteRunSweepIT extends ExecPostgresIT {

    private static final int THRESHOLD_SECONDS = 360;

    @Test
    void staleInProgressRun_isMarkedIncomplete_counted_andEmitsTerminalResultEvent() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        IncompleteRunSweeper sweeper = newSweeper(registry);

        UUID stale = insertRun("IN_PROGRESS", THRESHOLD_SECONDS + 60);   // older than threshold
        UUID fresh = insertRun("IN_PROGRESS", 5);                        // still within poll budget
        UUID done = insertRun("COMPLETED", THRESHOLD_SECONDS + 60);      // already terminal

        sweeper.sweep();

        assertThat(status(stale)).isEqualTo("INCOMPLETE");
        assertThat(finishedAt(stale)).as("terminal state records a finish time").isNotNull();
        assertThat(status(fresh)).as("in-flight run is not swept").isEqualTo("IN_PROGRESS");
        assertThat(status(done)).as("already-terminal run is untouched").isEqualTo("COMPLETED");
        assertThat(registry.counter("chaosforge.run.swept_incomplete_total").count()).isEqualTo(1.0);

        // The terminal-event half: exactly one PENDING result event for the swept run, none for the
        // others, and its Avro payload decodes to outcome=INCOMPLETE for the right run.
        assertThat(resultEventCount(stale)).as("swept run emitted its terminal result event").isEqualTo(1);
        assertThat(resultEventCount(fresh)).as("in-flight run emitted nothing").isZero();
        assertThat(resultEventCount(done)).as("already-terminal run emitted nothing").isZero();

        ScenarioRunResult event = decodeResultEvent(stale);
        assertThat(event.getScenarioId().toString()).isEqualTo(stale.toString());
        assertThat(event.getOutcome().toString()).isEqualTo("INCOMPLETE");
        assertThat(event.getReplayVersion()).isEqualTo(1L);
    }

    @Test
    void nothingStale_isNoOp() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        IncompleteRunSweeper sweeper = newSweeper(registry);
        insertRun("IN_PROGRESS", 5);

        sweeper.sweep();

        assertThat(registry.counter("chaosforge.run.swept_incomplete_total").count()).isEqualTo(0.0);
        Integer outboxRows = jdbc.queryForObject("SELECT count(*) FROM outbox", Integer.class);
        assertThat(outboxRows).as("no sweep → no result events").isZero();
    }

    /**
     * arch-audit H2 heartbeat: a run whose {@code started_at} is well past the threshold but whose
     * {@code last_attempt_at} is fresh — i.e. still bouncing through the DLQ retry lane, re-claimed
     * moments ago — must NOT be swept. Before V11 the sweep keyed on {@code started_at} and would have
     * prematurely marked this live run INCOMPLETE.
     */
    @Test
    void activelyRetriedRun_freshHeartbeat_isNotSwept() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        IncompleteRunSweeper sweeper = newSweeper(registry);

        UUID retrying = insertRunWithHeartbeat("IN_PROGRESS", THRESHOLD_SECONDS + 600, 5);

        sweeper.sweep();

        assertThat(status(retrying)).as("fresh heartbeat protects an actively-retried run")
                .isEqualTo("IN_PROGRESS");
        assertThat(resultEventCount(retrying)).isZero();
        assertThat(registry.counter("chaosforge.run.swept_incomplete_total").count()).isEqualTo(0.0);
    }

    /**
     * arch-audit H2 single-terminal-event: once the sweep marks a run INCOMPLETE, a late Phase-3
     * finalize (the retry finally succeeded) must be a no-op — the status stays INCOMPLETE and NO second
     * terminal result event is emitted. Before the status-guarded finalize this re-flipped to COMPLETED
     * and double-published a terminal event for one {@code (scenario_id, replay_version)}.
     */
    @Test
    void lateFinalizeAfterSweep_isNoOp_singleTerminalEvent() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunLogDao runLogDao = new RunLogDao(jdbc);
        FinalizePhase finalizePhase = new FinalizePhase(
                new ExecOutboxDao(jdbc), runLogDao, new AvroResultPayloadCodec());
        IncompleteRunSweeper sweeper = new IncompleteRunSweeper(runLogDao, finalizePhase,
                new ExecutionMetrics(registry), new DataSourceTransactionManager(ds));
        ReflectionTestUtils.setField(sweeper, "incompleteAfterSeconds", THRESHOLD_SECONDS);

        UUID scenarioId = insertRun("IN_PROGRESS", THRESHOLD_SECONDS + 60);
        sweeper.sweep();                                    // → INCOMPLETE + one terminal event
        assertThat(status(scenarioId)).isEqualTo("INCOMPLETE");

        // The retry finally lands COMPLETED, after the sweep already finalized the run.
        finalizePhase.complete(new io.chaosforge.execution.pipeline.ExecutionResult(
                scenarioId, UUID.randomUUID(), 1L, "COMPLETED"));

        assertThat(status(scenarioId)).as("late finalize must NOT resurrect the run").isEqualTo("INCOMPLETE");
        assertThat(resultEventCount(scenarioId)).as("exactly one terminal event survives").isEqualTo(1);
    }

    private IncompleteRunSweeper newSweeper(SimpleMeterRegistry registry) {
        RunLogDao runLogDao = new RunLogDao(jdbc);
        FinalizePhase finalizePhase = new FinalizePhase(
                new ExecOutboxDao(jdbc), runLogDao, new AvroResultPayloadCodec());
        IncompleteRunSweeper sweeper = new IncompleteRunSweeper(runLogDao, finalizePhase,
                new ExecutionMetrics(registry), new DataSourceTransactionManager(ds));
        ReflectionTestUtils.setField(sweeper, "incompleteAfterSeconds", THRESHOLD_SECONDS);
        return sweeper;
    }

    /** Aged run: both {@code started_at} and the heartbeat {@code last_attempt_at} are {@code ageSeconds} old. */
    private UUID insertRun(String status, int ageSeconds) {
        return insertRunWithHeartbeat(status, ageSeconds, ageSeconds);
    }

    /**
     * Seed a run with independent {@code started_at} and {@code last_attempt_at} ages (V11). Passing a
     * large start age with a small heartbeat age models a run that began long ago but was re-claimed
     * recently — the actively-retried case the sweep must leave alone.
     */
    private UUID insertRunWithHeartbeat(String status, int startAgeSeconds, int heartbeatAgeSeconds) {
        UUID scenarioId = UUID.randomUUID();
        jdbc.update("INSERT INTO scenario_run "
                        + "(scenario_id, replay_version, tenant_id, status, started_at, last_attempt_at) "
                        + "VALUES (?, ?, ?, ?, now() - make_interval(secs => ?), now() - make_interval(secs => ?))",
                scenarioId, 1L, UUID.randomUUID(), status, startAgeSeconds, heartbeatAgeSeconds);
        return scenarioId;
    }

    private String status(UUID scenarioId) {
        return jdbc.queryForObject("SELECT status FROM scenario_run WHERE scenario_id = ?", String.class, scenarioId);
    }

    private Object finishedAt(UUID scenarioId) {
        return jdbc.queryForObject("SELECT finished_at FROM scenario_run WHERE scenario_id = ?", Object.class, scenarioId);
    }

    private int resultEventCount(UUID scenarioId) {
        Integer c = jdbc.queryForObject("SELECT count(*) FROM outbox WHERE aggregate_id = ? "
                        + "AND topic = 'chaosforge.scenario.results.v1' AND status = 'PENDING'",
                Integer.class, scenarioId);
        return c == null ? -1 : c;
    }

    private ScenarioRunResult decodeResultEvent(UUID scenarioId) throws IOException {
        byte[] payload = jdbc.queryForObject("SELECT payload FROM outbox WHERE aggregate_id = ?",
                byte[].class, scenarioId);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(payload, null);
        return new SpecificDatumReader<ScenarioRunResult>(ScenarioRunResult.getClassSchema(),
                ScenarioRunResult.getClassSchema(), new ScenarioRunResult().getSpecificData())
                .read(null, decoder);
    }
}
