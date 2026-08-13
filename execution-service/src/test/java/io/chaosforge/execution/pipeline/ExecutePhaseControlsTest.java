package io.chaosforge.execution.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.chaosforge.execution.control.KillSwitch;
import io.chaosforge.execution.dlq.DlqRoutableException;
import io.chaosforge.execution.persistence.RunLogDao;
import io.chaosforge.execution.ruleset.CpRuleSetLoader;
import io.chaosforge.execution.ruleset.RuleSetRef;
import io.chaosforge.execution.ruleset.StepSpec;
import io.chaosforge.execution.steadystate.SteadyStateGuard;
import io.chaosforge.execution.steadystate.TargetHealthProbe;
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
 * The two executor stop-controls (acceptance gate C19), verified at the step boundary with mocked
 * collaborators (no infra — deterministic, and itself a demonstration that the controls touch neither
 * Postgres nor Kafka, so a partition of either cannot disable them):
 * <ul>
 *   <li>the {@link KillSwitch} aborts an in-flight scenario without issuing further fault steps;</li>
 *   <li>the aggregate deadline turns a slow/partitioned dependency into a bounded STEP_TIMEOUT.</li>
 * </ul>
 */
class ExecutePhaseControlsTest {

    private final CpRuleSetLoader ruleSetLoader = mock(CpRuleSetLoader.class);
    private final StepExecutor stepExecutor = mock(StepExecutor.class);
    private final RunLogDao runLogDao = mock(RunLogDao.class);
    private final KillSwitch killSwitch = new KillSwitch(new SimpleMeterRegistry());

    /** Default guard is disabled (enabled=false by default in a non-Spring instance) → never breaches. */
    private final SteadyStateGuard disabledGuard = new SteadyStateGuard(url -> true, new SimpleMeterRegistry());

    private ExecutePhase executePhase(long aggregateTimeoutMs) {
        return executePhase(aggregateTimeoutMs, disabledGuard);
    }

    private ExecutePhase executePhase(long aggregateTimeoutMs, SteadyStateGuard guard) {
        return new ExecutePhase(ruleSetLoader, stepExecutor, runLogDao, killSwitch, guard, aggregateTimeoutMs);
    }

    private static SteadyStateGuard enabledGuard(TargetHealthProbe probe, int maxConsecutiveFailures) {
        SteadyStateGuard guard = new SteadyStateGuard(probe, new SimpleMeterRegistry());
        ReflectionTestUtils.setField(guard, "enabled", true);
        ReflectionTestUtils.setField(guard, "maxConsecutiveFailures", maxConsecutiveFailures);
        ReflectionTestUtils.setField(guard, "healthPath", "/health");
        ReflectionTestUtils.setField(guard, "probeIntervalMs", 0L);   // no wall-clock delay in tests
        return guard;
    }

    @Test
    void killSwitchEngaged_abortsBeforeAnyStep_andIssuesNoFaultInjection() {
        givenSteps(step("s1"), step("s2"));
        killSwitch.engage("operator halt");

        ExecutionResult result = executePhase(240_000).execute(command(), 5L);

        assertThat(result.outcome()).isEqualTo("ABORTED");
        verify(stepExecutor, never()).execute(any(), anyLong(), any());   // no fault steps issued
        verify(runLogDao, never()).isStepCompleted(any(), anyLong(), any());
    }

    @Test
    void killSwitchEngagedMidScenario_runsEarlierSteps_thenAbortsAtTheNextBoundary() {
        givenSteps(step("s1"), step("s2"), step("s3"));
        when(runLogDao.isStepCompleted(any(), anyLong(), any())).thenReturn(false);
        // Step 1 runs and, as a side effect, the operator engages the switch — step 2 must not run.
        when(stepExecutor.execute(any(), anyLong(), any())).thenAnswer(inv -> {
            killSwitch.engage("halt after first step");
            return 200;
        });

        ExecutionResult result = executePhase(240_000).execute(command(), 5L);

        assertThat(result.outcome()).isEqualTo("ABORTED");
        verify(stepExecutor, times(1)).execute(any(), anyLong(), any());   // only step 1 issued
    }

    @Test
    void aggregateDeadlineExceeded_throwsStepTimeout_replayable() {
        givenSteps(step("s1"));
        // A deadline already in the past models a slow/partitioned dependency that blew the budget.
        assertThatThrownBy(() -> executePhase(-1L).execute(command(), 5L))
                .isInstanceOfSatisfying(DlqRoutableException.class, e -> {
                    assertThat(e.dlqReason()).isEqualTo("STEP_TIMEOUT");
                    assertThat(e.replayable()).isTrue();
                });
        verify(stepExecutor, never()).execute(any(), anyLong(), any());
    }

    @Test
    void steadyStateBreached_autoAbortsTheRun_andStopsInjectingFaults() {
        givenSteps(step("s1"), step("s2"));
        when(runLogDao.isStepCompleted(any(), anyLong(), any())).thenReturn(false);
        // Target health probe always unhealthy; tolerance 1 → breach on the first probe (before step 1).
        SteadyStateGuard guard = enabledGuard(url -> false, 1);

        ExecutionResult result = executePhase(240_000, guard).execute(command(), 5L);

        assertThat(result.outcome()).isEqualTo("ABORTED");
        verify(stepExecutor, never()).execute(any(), anyLong(), any());   // no further faults injected
    }

    @Test
    void steadyStateBreach_onASingleStepScenario_stillAborts() {
        // ADR-0533 regression: with the old once-per-step probe, a 1-step scenario could never reach a
        // 3-probe tolerance, so auto-abort was impossible for single-fault runs (the common case). The
        // time-based evaluation probes until resolved within the one boundary, so it now aborts.
        givenSteps(step("only"));
        when(runLogDao.isStepCompleted(any(), anyLong(), any())).thenReturn(false);
        SteadyStateGuard guard = enabledGuard(url -> false, 3);   // sustained unhealthy, tolerance 3

        ExecutionResult result = executePhase(240_000, guard).execute(command(), 5L);

        assertThat(result.outcome()).isEqualTo("ABORTED");
        verify(stepExecutor, never()).execute(any(), anyLong(), any());
    }

    @Test
    void steadyStateHealthy_runsToCompletion() {
        givenSteps(step("s1"), step("s2"));
        when(runLogDao.isStepCompleted(any(), anyLong(), any())).thenReturn(false);
        when(stepExecutor.execute(any(), anyLong(), any())).thenReturn(200);
        SteadyStateGuard guard = enabledGuard(url -> true, 3);   // target stays healthy

        ExecutionResult result = executePhase(240_000, guard).execute(command(), 5L);

        assertThat(result.outcome()).isEqualTo("COMPLETED");
        verify(stepExecutor, times(2)).execute(any(), anyLong(), any());
    }

    @Test
    void normalRun_noControlsTripped_executesEveryStep_completed() {
        givenSteps(step("s1"), step("s2"));
        when(runLogDao.isStepCompleted(any(), anyLong(), any())).thenReturn(false);
        when(stepExecutor.execute(any(), anyLong(), any())).thenReturn(200);

        ExecutionResult result = executePhase(240_000).execute(command(), 5L);

        assertThat(result.outcome()).isEqualTo("COMPLETED");
        verify(stepExecutor, times(2)).execute(any(), anyLong(), any());
    }

    private void givenSteps(StepSpec... steps) {
        when(ruleSetLoader.loadPinned(any(UUID.class), anyInt(), any(UUID.class)))
                .thenReturn(new RuleSetRef(UUID.randomUUID(), 1, List.of(steps)));
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
