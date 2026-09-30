package io.chaosforge.controlplane.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.chaosforge.controlplane.cache.TwoLevelCache;
import io.chaosforge.controlplane.domain.Scenario;
import io.chaosforge.controlplane.repository.ScenarioRepository;
import io.chaosforge.controlplane.security.TenantContext;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Keyset pagination boundary logic for {@link ScenarioService#list}. */
class ScenarioServiceTest {

    private final ScenarioRepository repository = mock(ScenarioRepository.class);
    @SuppressWarnings("unchecked")
    private final TwoLevelCache<Scenario> cache = mock(TwoLevelCache.class);
    private final ScenarioService service = new ScenarioService(repository, cache);

    private final UUID tenantId = UUID.randomUUID();

    @BeforeEach
    void bindTenant() {
        TenantContext.set(tenantId);
    }

    @AfterEach
    void clearTenant() {
        TenantContext.clear();
    }

    @Test
    void exactLimitPage_hasNoNextCursor() {
        // Repository is asked for limit+1; returning exactly `limit` rows means no next page.
        List<Scenario> rows = scenarios(3);
        when(repository.findPageByTenantId(eq(tenantId), isNull(), isNull(), eq(4))).thenReturn(rows);

        ScenarioService.ScenarioPage page = service.list(3, null);

        assertThat(page.items()).hasSize(3);
        assertThat(page.nextCursor()).isNull();
    }

    @Test
    void overflowPage_truncatesAndEmitsNextCursor() {
        // Repository returns limit+1 rows -> service must truncate to `limit` and derive a cursor
        // from the last item of the TRUNCATED page (not the extra lookahead row).
        List<Scenario> rows = scenarios(4);
        when(repository.findPageByTenantId(eq(tenantId), isNull(), isNull(), eq(4))).thenReturn(rows);

        ScenarioService.ScenarioPage page = service.list(3, null);

        assertThat(page.items()).hasSize(3);
        assertThat(page.items()).containsExactlyElementsOf(rows.subList(0, 3));
        assertThat(page.nextCursor()).isNotNull();
    }

    @Test
    void nonPositiveLimit_fallsBackToDefault() {
        when(repository.findPageByTenantId(eq(tenantId), isNull(), isNull(), eq(51))).thenReturn(List.of());

        service.list(0, null);

        verify(repository).findPageByTenantId(eq(tenantId), isNull(), isNull(), eq(51));
    }

    @Test
    void oversizedLimit_isClampedToMax() {
        when(repository.findPageByTenantId(eq(tenantId), isNull(), isNull(), eq(201))).thenReturn(List.of());

        service.list(9999, null);

        verify(repository).findPageByTenantId(eq(tenantId), isNull(), isNull(), eq(201));
    }

    @Test
    void validCursor_isDecodedIntoRepositoryArgs() {
        Scenario prior = scenario();
        String cursor = ScenarioPageCursor.encode(prior);
        when(repository.findPageByTenantId(any(), any(), any(), anyInt())).thenReturn(List.of());

        service.list(10, cursor);

        verify(repository).findPageByTenantId(
                eq(tenantId), eq(prior.createdAt()), eq(prior.scenarioId()), eq(11));
    }

    @Test
    void tamperedCursor_throwsIllegalArgumentException() {
        String garbage = Base64.getUrlEncoder().withoutPadding()
                .encodeToString("not-a-real-cursor".getBytes());

        assertThatThrownBy(() -> service.list(10, garbage))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private List<Scenario> scenarios(int count) {
        List<Scenario> result = new java.util.ArrayList<>();
        Instant base = Instant.now();
        for (int i = 0; i < count; i++) {
            result.add(new Scenario(UUID.randomUUID(), tenantId, "sc-" + i,
                    UUID.randomUUID(), 1, "PENDING", base.minusSeconds(i), base.minusSeconds(i)));
        }
        return result;
    }

    private Scenario scenario() {
        return scenarios(1).get(0);
    }
}
