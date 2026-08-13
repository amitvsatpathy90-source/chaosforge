package io.chaosforge.controlplane.domain;

import java.time.Instant;
import java.util.UUID;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

/**
 * Append-only (ADR-0503): identity is {@code (ruleSetId, version)}. {@code @Id} marks {@code ruleSetId}
 * for mapping metadata only — this aggregate is never written via {@code CrudRepository.save} (the
 * repository extends the bare {@code Repository<T,ID>}), so the non-unique surrogate id is never used.
 */
@Table("rule_sets")
public record RuleSet(
        @Id UUID ruleSetId,
        int version,
        UUID tenantId,
        String name,
        String definition,   // jsonb, read as text
        Instant createdAt) {
}
