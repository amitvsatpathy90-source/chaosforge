package io.chaosforge.execution.pipeline;

import io.chaosforge.execution.control.KillSwitch;
import io.chaosforge.execution.dlq.DlqRoutableException;
import io.chaosforge.execution.persistence.RunLogDao;
import io.chaosforge.execution.ruleset.CpRuleSetLoader;
import io.chaosforge.execution.ruleset.RuleSetRef;
import io.chaosforge.execution.ruleset.StepSpec;
import io.chaosforge.execution.steadystate.SteadyStateCheck;
import io.chaosforge.execution.steadystate.SteadyStateGuard;
import io.chaosforge.execution.step.StepExecutor;
import io.chaosforge.schema.v1.ScenarioRunCommand;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Phase 2 — execution (ADR-0523). No surrounding tx: each step's run-log row commits in its own
 * short tx, so partial progress survives a crash and is skipped on redelivery (isStepCompleted).
 * Three stop controls at each step boundary: {@link KillSwitch} (C19, operator abort, checked
 * first), aggregate deadline (bounded below max.poll.interval.ms → STEP_TIMEOUT), and
 * {@link SteadyStateCheck} (C20, target-health breach → auto-abort).
 */
@Component
public class ExecutePhase {

    private static final Logger log = LoggerFactory.getLogger(ExecutePhase.class);

    private final CpRuleSetLoader ruleSetLoader;
    private final StepExecutor stepExecutor;
    private final RunLogDao runLogDao;
    private final KillSwitch killSwitch;
    private final SteadyStateGuard steadyStateGuard;
    private final long aggregateTimeoutMs;

    public ExecutePhase(CpRuleSetLoader ruleSetLoader, StepExecutor stepExecutor, RunLogDao runLogDao,
                        KillSwitch killSwitch, SteadyStateGuard steadyStateGuard,
                        @Value("${chaosforge.step.aggregate-timeout-ms:240000}") long aggregateTimeoutMs) {
        this.ruleSetLoader = ruleSetLoader;
        this.stepExecutor = stepExecutor;
        this.runLogDao = runLogDao;
        this.killSwitch = killSwitch;
        this.steadyStateGuard = steadyStateGuard;
        this.aggregateTimeoutMs = aggregateTimeoutMs;
    }

    public ExecutionResult execute(ScenarioRunCommand command, long replayVersion) {
        UUID scenarioId = UUID.fromString(String.valueOf(command.getScenarioId()));
        UUID tenantId = UUID.fromString(String.valueOf(command.getTenantId()));
        UUID ruleSetId = UUID.fromString(String.valueOf(command.getRuleSetId()));

        // Step 4 — load by PINNED tuple, never latest (ADR-0503).
        RuleSetRef rules = ruleSetLoader.loadPinned(ruleSetId, command.getRuleSetVersion(), tenantId);
        SteadyStateCheck steadyState = steadyStateGuard.checkFor(rules);

        long deadline = System.currentTimeMillis() + aggregateTimeoutMs;
        String outcome = "COMPLETED";
        for (StepSpec step : rules.steps()) {
            if (killSwitch.isEngaged()) {
                // Emergency stop — do not issue further fault steps. Terminal ABORTED (no retry).
                log.warn("kill switch engaged — aborting scenario {} at step boundary (reason={})",
                        scenarioId, killSwitch.state().reason());
                outcome = "ABORTED";
                break;
            }
            if (System.currentTimeMillis() > deadline) {
                throw DlqRoutableException.stepTimeout("aggregate timeout for scenario " + scenarioId);
            }
            if (runLogDao.isStepCompleted(scenarioId, replayVersion, step.stepId())) {
                continue;   // already done in a prior partial execution
            }
            if (steadyState.breached(deadline)) {
                // Steady-state breach (C20). Log probe-target count only, never URLs.
                log.warn("steady-state breach across {} probe target(s) — auto-aborting scenario {}",
                        steadyState.healthUrls().size(), scenarioId);
                outcome = "ABORTED";
                break;
            }
            String idempotencyKey = scenarioId + ":" + replayVersion + ":" + step.stepId();
            String status = runStep(scenarioId, replayVersion, step);
            runLogDao.recordStep(scenarioId, replayVersion, step.stepId(), idempotencyKey, status);
            if ("FAILED".equals(status)) {
                outcome = "FAILED";
                break;
            }
        }
        // Terminal probe (ADR-0533): a completed run still gets one final steady-state check.
        if ("COMPLETED".equals(outcome) && steadyState.breached(deadline)) {
            log.warn("steady-state breach after final step — auto-aborting scenario {}", scenarioId);
            outcome = "ABORTED";
        }
        return new ExecutionResult(scenarioId, tenantId, replayVersion, outcome);
    }

    private String runStep(UUID scenarioId, long replayVersion, StepSpec step) {
        try {
            int httpStatus = stepExecutor.execute(scenarioId, replayVersion, step);
            return httpStatus >= 500 ? "FAILED" : "COMPLETED";
        } catch (DlqRoutableException e) {
            throw e;   // STEP_FAILED (malformed step) — not replayable
        } catch (CallNotPermittedException e) {
            // Step id only, never the target URL — these travel into DLQ headers and logs.
            throw DlqRoutableException.infraTransient("circuit open for step " + step.stepId(), e);
        } catch (RuntimeException e) {
            throw DlqRoutableException.infraTransient("target unreachable for step " + step.stepId(), e);
        }
    }
}
