package io.chaosforge.controlplane.domain;

import java.time.Instant;
import java.util.UUID;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Table("scenarios")
public record Scenario(
        @Id UUID scenarioId,
        UUID tenantId,
        String name,
        UUID ruleSetId,
        int ruleSetVersion,
        String status,
        Instant createdAt,
        Instant updatedAt) {
}
