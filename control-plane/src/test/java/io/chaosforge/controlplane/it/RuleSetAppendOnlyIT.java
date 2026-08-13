package io.chaosforge.controlplane.it;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;

/**
 * Rule sets are append-only (ADR-0503): the DB trigger {@code reject_rule_set_mutation} is the
 * structural backstop the suite asserts. Identity is {@code (rule_set_id, version)}; a new version is
 * a new row, never an UPDATE.
 */
class RuleSetAppendOnlyIT extends CpPostgresIT {

    @Test
    void updateOnExistingRuleSetVersion_isRejected() {
        UUID tenant = UUID.randomUUID(), ruleSet = UUID.randomUUID();
        jdbc.update("INSERT INTO tenants (tenant_id, name, rate_limit_per_min) VALUES (?, ?, ?)",
                tenant, "t", 600);
        jdbc.update("INSERT INTO rule_sets (rule_set_id, version, tenant_id, name, definition) "
                + "VALUES (?, ?, ?, ?, CAST(? AS jsonb))", ruleSet, 1, tenant, "rs", "{}");

        assertThatThrownBy(() -> jdbc.update(
                "UPDATE rule_sets SET name = ? WHERE rule_set_id = ? AND version = ?", "mutated", ruleSet, 1))
                .isInstanceOf(DataAccessException.class)
                .hasMessageContaining("append-only");
    }

    @Test
    void deleteOfRuleSetVersion_isRejected() {
        UUID tenant = UUID.randomUUID(), ruleSet = UUID.randomUUID();
        jdbc.update("INSERT INTO tenants (tenant_id, name, rate_limit_per_min) VALUES (?, ?, ?)",
                tenant, "t", 600);
        jdbc.update("INSERT INTO rule_sets (rule_set_id, version, tenant_id, name, definition) "
                + "VALUES (?, ?, ?, ?, CAST(? AS jsonb))", ruleSet, 1, tenant, "rs", "{}");

        assertThatThrownBy(() -> jdbc.update(
                "DELETE FROM rule_sets WHERE rule_set_id = ? AND version = ?", ruleSet, 1))
                .isInstanceOf(DataAccessException.class)
                .hasMessageContaining("append-only");
    }
}
