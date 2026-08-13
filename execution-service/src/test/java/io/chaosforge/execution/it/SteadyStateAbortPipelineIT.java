package io.chaosforge.execution.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.chaosforge.execution.control.KillSwitch;
import io.chaosforge.execution.persistence.ExecOutboxDao;
import io.chaosforge.execution.persistence.RunLogDao;
import io.chaosforge.execution.pipeline.ExecutePhase;
import io.chaosforge.execution.pipeline.ExecutionResult;
import io.chaosforge.execution.pipeline.FinalizePhase;
import io.chaosforge.execution.ruleset.CpRuleSetLoader;
import io.chaosforge.execution.ruleset.RuleSetRef;
import io.chaosforge.execution.ruleset.StepSpec;
import io.chaosforge.execution.steadystate.SteadyStateGuard;
import io.chaosforge.execution.step.StepExecutor;
import io.chaosforge.schema.v1.ScenarioRunCommand;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Acceptance gate <b>C20</b> end-to-end against real Postgres: a steady-state breach auto-aborts the
 * run and {@link FinalizePhase} persists it {@code ABORTED}. Also the regression guard for the V9
 * constraint fix — the V6 {@code scenario_run} CHECK rejected {@code ABORTED}, so without V9 this
 * finalize (and the C19 kill-switch finalize, whose unit tests used a mock DAO) would have failed in
 * production with a constraint violation and an unackable message.
 */
class SteadyStateAbortPipelineIT extends ExecPostgresIT {

    @Test
    void steadyStateBreach_abortsRun_andFinalizesScenarioRunAsAborted() {
        UUID scenarioId = UUID.randomUUID();
        UUID tenantId = UUID.randomUUID();
        long replayVersion = 5L;

        RunLogDao runLogDao = new RunLogDao(jdbc);
        runLogDao.insertRunHeader(scenarioId, replayVersion, tenantId);   // Phase 1 already ran

        CpRuleSetLoader ruleSetLoader = mock(CpRuleSetLoader.class);
        when(ruleSetLoader.loadPinned(any(), anyInt(), any()))
                .thenReturn(new RuleSetRef(UUID.randomUUID(), 1, List.of(step("s1"), step("s2"))));
        StepExecutor stepExecutor = mock(StepExecutor.class);   // must never run — target is unhealthy
        SteadyStateGuard guard = new SteadyStateGuard(url -> false, new SimpleMeterRegistry());
        ReflectionTestUtils.setField(guard, "enabled", true);
        ReflectionTestUtils.setField(guard, "maxConsecutiveFailures", 1);   // breach on the first probe
        ReflectionTestUtils.setField(guard, "healthPath", "/health");

        ExecutePhase executePhase = new ExecutePhase(
                ruleSetLoader, stepExecutor, runLogDao, new KillSwitch(new SimpleMeterRegistry()), guard, 240_000);

        ExecutionResult result = executePhase.execute(command(scenarioId, tenantId), replayVersion);

        assertThat(result.outcome()).isEqualTo("ABORTED");
        verify(stepExecutor, never()).execute(any(), anyLong(), any());

        // Phase 3 — finalize must persist ABORTED (the V9 CHECK now permits it).
        FinalizePhase finalizePhase = new FinalizePhase(
                new ExecOutboxDao(jdbc), runLogDao, new io.chaosforge.execution.pipeline.AvroResultPayloadCodec());
        tx.executeWithoutResult(s -> finalizePhase.complete(result));

        assertThat(runStatus(scenarioId, replayVersion)).isEqualTo("ABORTED");
        assertThat(resultEventCount(scenarioId)).as("a result event was emitted for the aborted run").isEqualTo(1);
    }

    private String runStatus(UUID scenarioId, long replayVersion) {
        return jdbc.queryForObject("SELECT status FROM scenario_run WHERE scenario_id = ? AND replay_version = ?",
                String.class, scenarioId, replayVersion);
    }

    private int resultEventCount(UUID scenarioId) {
        Integer c = jdbc.queryForObject("SELECT count(*) FROM outbox WHERE aggregate_id = ?",
                Integer.class, scenarioId);
        return c == null ? -1 : c;
    }

    private static StepSpec step(String id) {
        return new StepSpec(id, "LATENCY", "http://target/" + id, "GET", Map.of());
    }

    private static ScenarioRunCommand command(UUID scenarioId, UUID tenantId) {
        return ScenarioRunCommand.newBuilder()
                .setScenarioId(scenarioId.toString())
                .setTenantId(tenantId.toString())
                .setReplayVersion(5L)
                .setRuleSetId(UUID.randomUUID().toString())
                .setRuleSetVersion(1)
                .setSteps(List.of())
                .setCommandIssuedAt(Instant.now())
                .build();
    }
}
