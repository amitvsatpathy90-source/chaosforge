package io.chaosforge.execution.persistence;

import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Run header + per-step idempotency (ADR-0523). The {@code scenario_run_log} PK
 * {@code (scenario_id, replay_version, step_id)} + ON CONFLICT DO NOTHING is the step-idempotency
 * primitive that makes partial re-execution safe.
 */
@Component
public class RunLogDao {

    private final JdbcTemplate jdbc;

    public RunLogDao(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Insert the run header, or on redelivery advance {@code last_attempt_at} — the heartbeat
     * {@link #markStaleRunsIncomplete} uses to tell a wedged run from one still retrying.
     * {@code status} is never reset here: a terminal run isn't dragged back to IN_PROGRESS.
     */
    public void insertRunHeader(UUID scenarioId, long replayVersion, UUID tenantId) {
        jdbc.update("INSERT INTO scenario_run (scenario_id, replay_version, tenant_id, status) "
                + "VALUES (?, ?, ?, 'IN_PROGRESS') "
                + "ON CONFLICT (scenario_id, replay_version) DO UPDATE SET last_attempt_at = now()",
                scenarioId, replayVersion, tenantId);
    }

    public boolean isStepCompleted(UUID scenarioId, long replayVersion, String stepId) {
        Boolean exists = jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM scenario_run_log "
                        + "WHERE scenario_id = ? AND replay_version = ? AND step_id = ?)",
                Boolean.class, scenarioId, replayVersion, stepId);
        return Boolean.TRUE.equals(exists);
    }

    public void recordStep(UUID scenarioId, long replayVersion, String stepId, String idempotencyKey, String status) {
        jdbc.update("INSERT INTO scenario_run_log (scenario_id, replay_version, step_id, idempotency_key, status) "
                + "VALUES (?, ?, ?, ?, ?) ON CONFLICT (scenario_id, replay_version, step_id) DO NOTHING",
                scenarioId, replayVersion, stepId, idempotencyKey, status);
    }

    /**
     * Terminal transition, guarded to {@code status='IN_PROGRESS'} so it fires at most once (arch-audit
     * H2). Rows affected 1 = this call won, emit the event; 0 = already terminal, emit nothing.
     */
    public int finalizeRun(UUID scenarioId, long replayVersion, String status, String outcome) {
        return jdbc.update("UPDATE scenario_run SET status = ?, outcome = ?, finished_at = now() "
                + "WHERE scenario_id = ? AND replay_version = ? AND status = 'IN_PROGRESS'",
                status, outcome, scenarioId, replayVersion);
    }

    /**
     * Sweeps the ADR-0523 Phase-2→3 crash window: a run whose process died before finalize stays
     * IN_PROGRESS forever (no self-heal — its message_id is inbox-deduped). Marks such runs
     * INCOMPLETE once older than {@code olderThanSeconds} (must exceed max.poll.interval.ms + grace).
     * Returns swept identities via RETURNING (atomic with the flip) so the caller emits one terminal
     * event per run. Staleness is on {@code last_attempt_at}, not {@code started_at} (arch-audit H2) —
     * a run actively retrying keeps its heartbeat fresh and is never swept mid-retry.
     */
    public List<SweptRun> markStaleRunsIncomplete(int olderThanSeconds) {
        return jdbc.query("UPDATE scenario_run SET status = 'INCOMPLETE', finished_at = now() "
                + "WHERE status = 'IN_PROGRESS' AND last_attempt_at < now() - make_interval(secs => ?) "
                + "RETURNING scenario_id, replay_version, tenant_id",
                (rs, n) -> new SweptRun(
                        rs.getObject("scenario_id", UUID.class),
                        rs.getLong("replay_version"),
                        rs.getObject("tenant_id", UUID.class)),
                olderThanSeconds);
    }

    /** Identity of a run the sweep just flipped to {@code INCOMPLETE}. */
    public record SweptRun(UUID scenarioId, long replayVersion, UUID tenantId) {}
}
