package io.chaosforge.execution.it;

import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;

import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/**
 * Database-level regression for the scenario_run_log per-step idempotency primitive (ADR-0523):
 * a duplicate {@code (scenario_id, replay_version, step_id)} must be rejected by the real
 * Postgres primary key. Raw JDBC inserts bypass {@code RunLogDao}'s {@code ON CONFLICT}
 * handling so a weakened database constraint cannot be masked.
 */
class ScenarioRunLogConstraintIT extends ExecPostgresIT {

    @Test
    void duplicateRunLogEntry_isRejected() {
        UUID scenarioId = UUID.randomUUID();
        UUID tenantId = UUID.randomUUID();
        long replayVersion = 1L;
        String stepId = "step-1";

        // scenario_run_log references the run header by (scenario_id, replay_version).
        jdbc.update(
                "INSERT INTO scenario_run (scenario_id, replay_version, tenant_id) VALUES (?, ?, ?)",
                scenarioId, replayVersion, tenantId);

        jdbc.update(
                "INSERT INTO scenario_run_log "
                        + "(scenario_id, replay_version, step_id, idempotency_key, status) "
                        + "VALUES (?, ?, ?, ?, ?)",
                scenarioId,
                replayVersion,
                stepId,
                scenarioId + ":" + replayVersion + ":" + stepId,
                "COMPLETED");

        // Bypass RunLogDao's ON CONFLICT handling so the database primary key is exercised directly.
        assertThatThrownBy(() -> jdbc.update(
                "INSERT INTO scenario_run_log "
                        + "(scenario_id, replay_version, step_id, idempotency_key, status) "
                        + "VALUES (?, ?, ?, ?, ?)",
                scenarioId,
                replayVersion,
                stepId,
                scenarioId + ":" + replayVersion + ":" + stepId,
                "COMPLETED"))
                .isInstanceOf(DataIntegrityViolationException.class);
    }
}
