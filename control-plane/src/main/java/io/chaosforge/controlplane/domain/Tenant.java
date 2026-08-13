package io.chaosforge.controlplane.domain;

import java.time.Instant;
import java.util.UUID;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Table("tenants")
public record Tenant(
        @Id UUID tenantId,
        String name,
        String status,
        int rateLimitPerMin,
        Instant createdAt,
        Instant updatedAt) {
}
