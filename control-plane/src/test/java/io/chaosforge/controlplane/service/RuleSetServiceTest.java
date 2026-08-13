package io.chaosforge.controlplane.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.chaosforge.common.target.TargetUrlGuard;
import io.chaosforge.controlplane.ai.TargetNotOwnedException;
import io.chaosforge.controlplane.domain.RuleSet;
import io.chaosforge.controlplane.repository.RuleSetRepository;
import io.chaosforge.controlplane.security.TenantContext;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

/**
 * Proves the authoring-side SSRF/blast-radius guard (ADR-0534) actually fires on
 * {@link RuleSetService#create} — the early, UX-oriented HTTP 422 check, as distinct from the
 * execution-side terminal {@code STEP_FAILED} covered by {@code CpRuleSetLoaderTest}. {@code
 * TargetUrlGuardTest} exercises the guard class in isolation; nothing previously proved that {@code
 * create} extracts every {@code targetUrl} from the definition JSON, guards it before ever touching
 * the repository, and leaves a definition with no parseable JSON untouched (the executor's guard is
 * the backstop for that case).
 */
class RuleSetServiceTest {

    private final RuleSetRepository repository = mock(RuleSetRepository.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @AfterEach
    void clearTenant() {
        TenantContext.clear();
    }

    @Test
    void create_definitionTargetingMetadataHost_throwsTargetNotOwnedException_andNeverPersists() {
        TenantContext.set(UUID.randomUUID());
        RuleSetService service = new RuleSetService(repository, new TargetUrlGuard(true, List.of()), objectMapper);
        String definition = "{\"steps\":[{\"targetUrl\":\"http://169.254.169.254/latest/meta-data/\"}]}";

        assertThatThrownBy(() -> service.create("rs", definition))
                .isInstanceOf(TargetNotOwnedException.class);

        verify(repository, never()).insert(any(), anyInt(), any(), any(), any());
    }

    @Test
    void create_definitionTargetingAllowlistedHost_persistsAndReturnsTheRow() {
        UUID tenant = UUID.randomUUID();
        TenantContext.set(tenant);
        RuleSetService service = new RuleSetService(
                repository, new TargetUrlGuard(true, List.of("example.com")), objectMapper);
        String definition = "{\"steps\":[{\"targetUrl\":\"https://example.com/api\"}]}";
        RuleSet persisted = new RuleSet(null, 1, tenant, "rs", definition, Instant.now());
        when(repository.findByRuleSetIdAndVersionAndTenantId(any(), eq(1), eq(tenant)))
                .thenReturn(Optional.of(persisted));

        RuleSet result = service.create("rs", definition);

        assertThat(result).isEqualTo(persisted);
        verify(repository).insert(any(), eq(1), eq(tenant), eq("rs"), eq(definition));
    }

    @Test
    void create_definitionThatIsNotJson_skipsValidation_stillPersists() {
        UUID tenant = UUID.randomUUID();
        TenantContext.set(tenant);
        RuleSetService service = new RuleSetService(repository, new TargetUrlGuard(true, List.of()), objectMapper);
        String definition = "not json at all";
        RuleSet persisted = new RuleSet(null, 1, tenant, "rs", definition, Instant.now());
        when(repository.findByRuleSetIdAndVersionAndTenantId(any(), eq(1), eq(tenant)))
                .thenReturn(Optional.of(persisted));

        RuleSet result = service.create("rs", definition);

        assertThat(result).isEqualTo(persisted);
        verify(repository).insert(any(), eq(1), eq(tenant), eq("rs"), eq(definition));
    }
}
