package io.chaosforge.controlplane.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.chaosforge.controlplane.domain.RunProjection;
import io.chaosforge.controlplane.repository.RunProjectionRepository;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

/** Upsert idempotency and tenant-scoped lookup for the run-result cache, real Postgres. */
class RunProjectionRepositoryIT extends AbstractCpIntegrationTest {

    @Autowired
    private RunProjectionRepository repository;

    @Autowired
    private JdbcTemplate jdbc;

    private final UUID scenario = UUID.randomUUID();
    private final UUID tenant = UUID.randomUUID();
    private final Instant t0 = Instant.now().truncatedTo(ChronoUnit.MICROS);   // PG stores micros

    @Test
    void upsert_redeliveryOfSameEvent_isNoOpOnPayload() {
        repository.upsert(scenario, 1L, tenant, "COMPLETED", t0);
        repository.upsert(scenario, 1L, tenant, "COMPLETED", t0);   // Kafka redelivery

        assertThat(rowCount(scenario)).isEqualTo(1);
        RunProjection row = find(scenario, 1L, tenant);
        assertThat(row.outcome()).isEqualTo("COMPLETED");
        assertThat(row.finishedAt()).isEqualTo(t0);
    }

    // Exec emits one terminal event per run, so a differing outcome is an upstream anomaly; last write wins.
    @Test
    void upsert_differentOutcomeSameKey_lastWriteWins() {
        repository.upsert(scenario, 1L, tenant, "INCOMPLETE", t0);
        repository.upsert(scenario, 1L, tenant, "COMPLETED", t0.plusSeconds(5));

        assertThat(rowCount(scenario)).isEqualTo(1);
        RunProjection row = find(scenario, 1L, tenant);
        assertThat(row.outcome()).isEqualTo("COMPLETED");
        assertThat(row.finishedAt()).isEqualTo(t0.plusSeconds(5));
    }

    @Test
    void distinctReplayVersions_areDistinctRows() {
        repository.upsert(scenario, 1L, tenant, "FAILED", t0);
        repository.upsert(scenario, 2L, tenant, "COMPLETED", t0.plusSeconds(60));

        assertThat(rowCount(scenario)).isEqualTo(2);
        assertThat(find(scenario, 1L, tenant).outcome()).isEqualTo("FAILED");
        assertThat(find(scenario, 2L, tenant).outcome()).isEqualTo("COMPLETED");
    }

    // outcome is a free string by design: a future terminal state must not need a CP change.
    @Test
    void upsert_unknownFutureOutcome_isStoredVerbatim() {
        repository.upsert(scenario, 1L, tenant, "SOME_FUTURE_STATE", t0);

        assertThat(find(scenario, 1L, tenant).outcome()).isEqualTo("SOME_FUTURE_STATE");
    }

    @Test
    void find_ownerHit_returnsAllFields() {
        repository.upsert(scenario, 7L, tenant, "ABORTED", t0);

        RunProjection row = find(scenario, 7L, tenant);
        assertThat(row.scenarioId()).isEqualTo(scenario);
        assertThat(row.replayVersion()).isEqualTo(7L);
        assertThat(row.tenantId()).isEqualTo(tenant);
        assertThat(row.updatedAt()).isNotNull();
    }

    // ADR-0510: a cross-tenant miss and a nonexistent key are the same empty result.
    @Test
    void find_crossTenantAndNonexistent_bothEmpty() {
        repository.upsert(scenario, 1L, tenant, "COMPLETED", t0);

        assertThat(repository.findByScenarioIdAndReplayVersionAndTenantId(
                scenario, 1L, UUID.randomUUID())).isEmpty();
        assertThat(repository.findByScenarioIdAndReplayVersionAndTenantId(
                UUID.randomUUID(), 1L, tenant)).isEmpty();
        assertThat(repository.findByScenarioIdAndReplayVersionAndTenantId(
                scenario, 99L, tenant)).isEmpty();
    }

    private RunProjection find(UUID scenarioId, long version, UUID tenantId) {
        return repository.findByScenarioIdAndReplayVersionAndTenantId(scenarioId, version, tenantId)
                .orElseThrow();
    }

    private int rowCount(UUID scenarioId) {
        return jdbc.queryForObject(
                "SELECT count(*) FROM run_projection WHERE scenario_id = ?", Integer.class, scenarioId);
    }
}
