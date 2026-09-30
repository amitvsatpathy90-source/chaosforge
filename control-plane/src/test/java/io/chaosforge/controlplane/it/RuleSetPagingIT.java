package io.chaosforge.controlplane.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.chaosforge.controlplane.domain.RuleSet;
import io.chaosforge.controlplane.repository.RuleSetRepository;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

class RuleSetPagingIT extends AbstractCpIntegrationTest {

    @Autowired
    private RuleSetRepository repository;

    @Autowired
    private JdbcTemplate jdbc;

    @Test
    void keysetPaging_visitsEveryRowOnce_acrossCreatedAtTies() {
        UUID tenant = UUID.randomUUID();
        jdbc.update("INSERT INTO tenants (tenant_id, name, rate_limit_per_min) VALUES (?, ?, ?)",
                tenant, "t", 600);
        Instant t0 = Instant.now().minusSeconds(60).truncatedTo(ChronoUnit.MICROS);   // PG stores micros
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        insert(tenant, a, 1, t0);
        insert(tenant, a, 2, t0);   // tie: same created_at, differs by version
        insert(tenant, b, 1, t0);   // tie: differs by rule_set_id
        insert(tenant, UUID.randomUUID(), 1, t0.plusSeconds(1));
        insert(tenant, UUID.randomUUID(), 1, t0.plusSeconds(2));

        List<RuleSet> all = repository.findPageByTenantId(
                tenant, null, null, null, 100);
        assertThat(all).hasSize(5);
        assertThat(all).allSatisfy(ruleSet -> assertThat(ruleSet.definition()).isNull());   // metadata only

        List<RuleSet> pageList = new ArrayList<>();
        RuleSet last = null;
        while (true) {
            List<RuleSet> currentPage = last == null
                    ? repository.findPageByTenantId(
                            tenant, null, null, null, 2)
                    : repository.findPageByTenantId(
                            tenant, last.createdAt(), last.ruleSetId(), last.version(), 2);
            if (currentPage.isEmpty()) {
                break;
            }
            pageList.addAll(currentPage);
            last = currentPage.getLast();
        }
        assertThat(pageList).containsExactlyElementsOf(all);   // no gap, no duplicate

        assertThat(repository.findPageByTenantId(UUID.randomUUID(), null, null, null, 100)).isEmpty();
    }

    private void insert(UUID tenant, UUID id, int version, Instant createdAt) {
        jdbc.update("INSERT INTO rule_sets (rule_set_id, version, tenant_id, name, definition, created_at) "
                        + "VALUES (?, ?, ?, ?, CAST(? AS jsonb), ?)",
                id, version, tenant, "rs", "{}", Timestamp.from(createdAt));
    }
}
