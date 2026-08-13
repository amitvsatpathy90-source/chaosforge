package io.chaosforge.controlplane.web;

import io.chaosforge.controlplane.domain.Tenant;
import io.chaosforge.controlplane.service.TenantService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import java.util.UUID;
import org.jspecify.annotations.Nullable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/tenants")
public class TenantController {

    // New-tenant default. The gateway mirrors this value as its fail-open fallback
    // (edge-gateway TenantPolicy.DEFAULT_RATE_LIMIT_PER_MIN) — keep the two in sync (arch-audit L3).
    private static final int DEFAULT_RATE_LIMIT_PER_MIN = 600;

    private final TenantService tenantService;

    public TenantController(TenantService tenantService) {
        this.tenantService = tenantService;
    }

    @PostMapping
    public ResponseEntity<TenantResponse> create(@Valid @RequestBody CreateTenantRequest req) {
        int limit = req.rateLimitPerMin() == null ? DEFAULT_RATE_LIMIT_PER_MIN : req.rateLimitPerMin();
        Tenant t = tenantService.create(req.name(), limit);
        return ResponseEntity.status(HttpStatus.CREATED).body(toResponse(t));
    }

    @GetMapping("/{tenantId}")
    public TenantResponse get(@PathVariable UUID tenantId) {
        return toResponse(tenantService.get(tenantId));
    }

    /** Mutation path that exercises explicit cache invalidation (ADR-0504). */
    @PatchMapping("/{tenantId}/rate-limit")
    public TenantResponse updateRateLimit(@PathVariable UUID tenantId,
                                          @Valid @RequestBody UpdateRateLimitRequest req) {
        return toResponse(tenantService.updateRateLimit(tenantId, req.rateLimitPerMin()));
    }

    private static TenantResponse toResponse(Tenant t) {
        return new TenantResponse(t.tenantId(), t.name(), t.status(), t.rateLimitPerMin());
    }

    public record CreateTenantRequest(@NotBlank String name, @Positive @Nullable Integer rateLimitPerMin) {}

    public record UpdateRateLimitRequest(@Positive int rateLimitPerMin) {}

    public record TenantResponse(UUID tenantId, String name, String status, int rateLimitPerMin) {}
}
