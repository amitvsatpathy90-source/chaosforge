package io.chaosforge.controlplane.domain;

import java.time.Instant;
import java.util.UUID;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

/**
 * Read cache of one terminal {@code ScenarioRunResult} (Batch 4). Identity is
 * {@code (scenarioId, replayVersion)}; {@code @Id} on scenarioId is mapping metadata only — this
 * repository extends the bare {@code Repository<T,ID>}, never {@code CrudRepository.save}.
 */
@Table("run_projection")
public record RunProjection(
        @Id UUID scenarioId,
        long replayVersion,
        UUID tenantId,
        String outcome,       // mirrors ScenarioRunResult.outcome; not an enum, see V12 migration
        Instant finishedAt,
        Instant updatedAt) {
}
