package io.chaosforge.controlplane.service;

import com.github.f4b6a3.uuid.UuidCreator;
import io.chaosforge.controlplane.cache.TwoLevelCache;
import io.chaosforge.controlplane.domain.Scenario;
import io.chaosforge.controlplane.error.ResourceNotFoundException;
import io.chaosforge.controlplane.repository.ScenarioRepository;
import io.chaosforge.controlplane.security.TenantContext;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class ScenarioService {

    private final ScenarioRepository repository;
    private final TwoLevelCache<Scenario> scenarioCache;

    private static final int DEFAULT_PAGE_SIZE = 50;
    private static final int MAX_PAGE_SIZE = 200;

    public ScenarioService(ScenarioRepository repository, TwoLevelCache<Scenario> scenarioCache) {
        this.repository = repository;
        this.scenarioCache = scenarioCache;
    }

    public Scenario create(String name, UUID ruleSetId, int ruleSetVersion) {
        UUID tenantId = TenantContext.require();
        UUID scenarioId = UuidCreator.getTimeOrderedEpoch();
        repository.insert(scenarioId, tenantId, name, ruleSetId, ruleSetVersion);   // seed trigger creates replay_state row
        return repository.findByScenarioIdAndTenantId(scenarioId, tenantId).orElseThrow();
    }

    public Scenario get(UUID scenarioId) {
        UUID tenantId = TenantContext.require();
        // Tenant-scoped cache key — keying by scenarioId alone would leak across tenants.
        String key = tenantId + ":" + scenarioId;
        return scenarioCache.get(key,
                k -> repository.findByScenarioIdAndTenantId(scenarioId, tenantId)
                        .orElseThrow(() -> new ResourceNotFoundException(scenarioId)));
    }

    public ScenarioPage list (int limit, String cursor) {
        UUID tenantId = TenantContext.require();
        int boundedLimit = limit<=0?DEFAULT_PAGE_SIZE:Math.min(limit, MAX_PAGE_SIZE);

        Instant cursorCreatedAt = null;
        UUID cursorScenarioId = null;

        if (cursor != null && !cursor.isBlank()) {
            ScenarioPageCursor decodedCursor = ScenarioPageCursor.decode(cursor);
            cursorCreatedAt = decodedCursor.createdAt();
            cursorScenarioId = decodedCursor.scenarioId();
        }

        // Fetch one extra row to detect a next page without a second round-trip.
        List<Scenario> rows =
                repository.findPageByTenantId(tenantId, cursorCreatedAt, cursorScenarioId, boundedLimit + 1);

        boolean hasMore = rows.size() > boundedLimit;
        List<Scenario> page = hasMore ? rows.subList(0, boundedLimit) : rows;
        String nextCursor = hasMore ? ScenarioPageCursor.encode(page.getLast()) : null;

        return new ScenarioPage(page, nextCursor);
    }

    public record ScenarioPage(List<Scenario> items, String nextCursor){}

    /** Current fencing token for the ETag — read FRESH (never cached); it changes on every replay. */
    public long replayVersion(UUID scenarioId) {
        UUID tenantId = TenantContext.require();
        return repository.findReplayVersion(scenarioId, tenantId)
                .orElseThrow(() -> new ResourceNotFoundException(scenarioId));
    }
}
