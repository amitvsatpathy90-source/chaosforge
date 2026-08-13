package io.chaosforge.execution.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.f4b6a3.uuid.UuidCreator;
import io.chaosforge.execution.dlq.DlqRoutableException;
import io.chaosforge.execution.persistence.InboxDao;
import io.chaosforge.execution.persistence.InboxFenceDao;
import io.chaosforge.execution.persistence.RunLogDao;
import io.chaosforge.execution.pipeline.ClaimPhase;
import io.chaosforge.execution.pipeline.ClaimResult;
import io.chaosforge.schema.v1.ScenarioRunCommand;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Phase-1 claim invariants against real Postgres (ADR-0523): inbox dedup, and the load-bearing
 * ordering that fencing PRECEDES inbox — so a stale (superseded) version is DLQ'd as FENCING_VIOLATION
 * without ever marking its {@code message_id} processed.
 */
class ClaimPhaseIT extends ExecPostgresIT {

    private ClaimPhase claimPhase() {
        return new ClaimPhase(new InboxFenceDao(jdbc), new InboxDao(jdbc), new RunLogDao(jdbc));
    }

    @Test
    void firstClaim_proceeds_andAdvancesFence() {
        ClaimPhase claim = claimPhase();
        // message_id is a UUIDv7 by construction (its embedded time is the inbox partition key, C28).
        UUID scenario = UUID.randomUUID(), tenant = UUID.randomUUID(), msg = UuidCreator.getTimeOrderedEpoch();
        ScenarioRunCommand cmd = command(scenario, tenant);

        ClaimResult result = tx.execute(s -> claim.claim(cmd, msg, 5));

        assertThat(result.isDuplicate()).isFalse();
        assertThat(maxSeen(scenario)).isEqualTo(5);
        assertThat(inboxCount(msg)).isEqualTo(1);
        assertThat(runHeaderCount(scenario)).isEqualTo(1);
    }

    @Test
    void duplicateMessageId_isDeduped_withNoSecondEffect() {
        ClaimPhase claim = claimPhase();
        UUID scenario = UUID.randomUUID(), tenant = UUID.randomUUID(), msg = UuidCreator.getTimeOrderedEpoch();
        ScenarioRunCommand cmd = command(scenario, tenant);

        tx.execute(s -> claim.claim(cmd, msg, 5));                 // first sight → PROCEED
        ClaimResult redelivery = tx.execute(s -> claim.claim(cmd, msg, 5));   // same version redelivery

        // Same-version redelivery falls through to inbox dedup (NOT fencing) → deduped, not DLQ'd.
        assertThat(redelivery.isDuplicate()).isTrue();
        assertThat(inboxCount(msg)).isEqualTo(1);                  // exactly one effect
        assertThat(runHeaderCount(scenario)).isEqualTo(1);
    }

    @Test
    void staleVersion_throwsFencingViolation_beforeInbox() {
        ClaimPhase claim = claimPhase();
        UUID scenario = UUID.randomUUID(), tenant = UUID.randomUUID();
        ScenarioRunCommand cmd = command(scenario, tenant);
        tx.execute(s -> claim.claim(cmd, UuidCreator.getTimeOrderedEpoch(), 5));   // advance max_seen to 5

        UUID staleMsg = UuidCreator.getTimeOrderedEpoch();
        assertThatThrownBy(() -> tx.execute(s -> claim.claim(cmd, staleMsg, 3)))
                .isInstanceOfSatisfying(DlqRoutableException.class, e -> {
                    assertThat(e.dlqReason()).isEqualTo("FENCING_VIOLATION");
                    assertThat(e.replayable()).isFalse();
                });
        // Fencing precedes inbox: the stale message_id was NOT recorded, so a later higher-version
        // replay of the same message_id is not silently blocked.
        assertThat(inboxCount(staleMsg)).isZero();
        assertThat(maxSeen(scenario)).isEqualTo(5);
    }

    private long maxSeen(UUID scenario) {
        Long v = jdbc.queryForObject("SELECT max_seen FROM inbox_fence WHERE scenario_id = ?", Long.class, scenario);
        return v == null ? -99L : v;
    }

    private int inboxCount(UUID messageId) {
        Integer c = jdbc.queryForObject("SELECT count(*) FROM inbox WHERE message_id = ?", Integer.class, messageId);
        return c == null ? -1 : c;
    }

    private int runHeaderCount(UUID scenario) {
        Integer c = jdbc.queryForObject("SELECT count(*) FROM scenario_run WHERE scenario_id = ?", Integer.class, scenario);
        return c == null ? -1 : c;
    }

    private static ScenarioRunCommand command(UUID scenarioId, UUID tenantId) {
        return ScenarioRunCommand.newBuilder()
                .setScenarioId(scenarioId.toString())
                .setTenantId(tenantId.toString())
                .setReplayVersion(0L)
                .setRuleSetId(UUID.randomUUID().toString())
                .setRuleSetVersion(1)
                .setSteps(List.of())
                .setCommandIssuedAt(Instant.now())
                .build();
    }
}
