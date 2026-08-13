package io.chaosforge.execution.sweep;

import io.chaosforge.execution.observability.ExecutionMetrics;
import io.chaosforge.execution.persistence.RunLogDao;
import io.chaosforge.execution.persistence.RunLogDao.SweptRun;
import io.chaosforge.execution.pipeline.ExecutionResult;
import io.chaosforge.execution.pipeline.FinalizePhase;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Background sweep for the ADR-0523 Phase-2→3 crash window: a run that never reached finalize sits
 * IN_PROGRESS forever (inbox-deduped, no self-heal). Marks it INCOMPLETE and emits a terminal result
 * event in one tx, so a downstream consumer sees the same contract as a normal finalize.
 * Threshold must exceed max.poll.interval.ms + grace so an in-flight run is never swept.
 */
@Component
public class IncompleteRunSweeper {

    private static final Logger log = LoggerFactory.getLogger(IncompleteRunSweeper.class);

    private final RunLogDao runLogDao;
    private final FinalizePhase finalizePhase;
    private final ExecutionMetrics metrics;
    private final TransactionTemplate tx;

    @Value("${chaosforge.run.sweep.incomplete-after-seconds:360}")
    private int incompleteAfterSeconds;

    public IncompleteRunSweeper(RunLogDao runLogDao, FinalizePhase finalizePhase,
                                ExecutionMetrics metrics, PlatformTransactionManager txManager) {
        this.runLogDao = runLogDao;
        this.finalizePhase = finalizePhase;
        this.metrics = metrics;
        this.tx = new TransactionTemplate(txManager);
    }

    @Scheduled(fixedDelayString = "${chaosforge.run.sweep.interval-ms:60000}")
    public void sweep() {
        Instant finishedAt = Instant.now();
        List<SweptRun> swept = tx.execute(status -> {
            // RETURNING makes flip + event atomic; only still-IN_PROGRESS rows are flipped and emit.
            List<SweptRun> rows = runLogDao.markStaleRunsIncomplete(incompleteAfterSeconds);
            for (SweptRun run : rows) {
                finalizePhase.insertResultEvent(new ExecutionResult(
                        run.scenarioId(), run.tenantId(), run.replayVersion(), "INCOMPLETE"), finishedAt);
            }
            return rows;
        });
        if (swept != null && !swept.isEmpty()) {
            metrics.sweptIncomplete(swept.size());
            // No tenant_id / scenario_id here — aggregate count only (PII + cardinality discipline).
            log.warn("swept {} stale run(s) to INCOMPLETE + emitted terminal result event(s) "
                    + "(older than {}s, Phase-2->3 crash window)", swept.size(), incompleteAfterSeconds);
        }
    }
}
