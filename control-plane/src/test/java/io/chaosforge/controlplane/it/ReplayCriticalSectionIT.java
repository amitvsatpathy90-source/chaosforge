package io.chaosforge.controlplane.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.chaosforge.controlplane.error.ResourceNotFoundException;
import io.chaosforge.controlplane.replay.ConcurrentReplayException;
import io.chaosforge.controlplane.replay.ReplayCommand;
import io.chaosforge.controlplane.replay.ReplayToken;
import io.chaosforge.controlplane.replay.ScenarioReplayOrchestrator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * The ADR-0528 ownership-first CAS critical section against real Postgres row locking. Drives the
 * real {@link ScenarioReplayOrchestrator}; no mocks of the DB.
 */
class ReplayCriticalSectionIT extends CpPostgresIT {

    @Test
    void newClaim_bumpsVersion_andReturnsFencingToken() {
        UUID tenant = UUID.randomUUID(), ruleSet = UUID.randomUUID(), scenario = UUID.randomUUID();
        seed(tenant, ruleSet, 1, scenario);
        ScenarioReplayOrchestrator orch = newOrchestrator();

        ReplayToken token = tx.execute(s ->
                orch.initiateReplay(new ReplayCommand(scenario, tenant, 0, UUID.randomUUID())));

        assertThat(token.fencingToken()).isEqualTo(1);
        assertThat(token.idempotentReplay()).isFalse();
        assertThat(token.ruleSetVersion()).isEqualTo(1);
        assertThat(replayVersionOf(scenario)).isEqualTo(1);
    }

    @Test
    void staleExpectedVersion_throwsConcurrentReplay_andDoesNotBump() {
        UUID tenant = UUID.randomUUID(), ruleSet = UUID.randomUUID(), scenario = UUID.randomUUID();
        seed(tenant, ruleSet, 1, scenario);
        ScenarioReplayOrchestrator orch = newOrchestrator();
        // First claim moves version 0 → 1.
        tx.execute(s -> orch.initiateReplay(new ReplayCommand(scenario, tenant, 0, UUID.randomUUID())));

        // Second claim with the now-stale If-Match=0 → 409, version unchanged.
        assertThatThrownBy(() -> tx.execute(s ->
                orch.initiateReplay(new ReplayCommand(scenario, tenant, 0, UUID.randomUUID()))))
                .isInstanceOf(ConcurrentReplayException.class);
        assertThat(replayVersionOf(scenario)).isEqualTo(1);
    }

    @Test
    void crossTenant_throwsNotFound_andNeverReachesCas() {
        UUID owner = UUID.randomUUID(), ruleSet = UUID.randomUUID(), scenario = UUID.randomUUID();
        seed(owner, ruleSet, 1, scenario);
        UUID attacker = UUID.randomUUID();
        ScenarioReplayOrchestrator orch = newOrchestrator();

        // Ownership probe (step 1) is empty → 404 before the CAS; indistinguishable from "absent".
        assertThatThrownBy(() -> tx.execute(s ->
                orch.initiateReplay(new ReplayCommand(scenario, attacker, 0, UUID.randomUUID()))))
                .isInstanceOf(ResourceNotFoundException.class);
        assertThat(replayVersionOf(scenario)).isEqualTo(0);   // no write side-effect on the 404 path
    }

    @Test
    void replayedIdempotencyKey_returnsOriginalToken_withoutSecondBump() {
        UUID tenant = UUID.randomUUID(), ruleSet = UUID.randomUUID(), scenario = UUID.randomUUID();
        seed(tenant, ruleSet, 1, scenario);
        ScenarioReplayOrchestrator orch = newOrchestrator();
        UUID idempotencyKey = UUID.randomUUID();

        ReplayToken first = tx.execute(s ->
                orch.initiateReplay(new ReplayCommand(scenario, tenant, 0, idempotencyKey)));
        ReplayToken replay = tx.execute(s ->
                orch.initiateReplay(new ReplayCommand(scenario, tenant, 0, idempotencyKey)));

        assertThat(first.idempotentReplay()).isFalse();
        assertThat(replay.idempotentReplay()).isTrue();
        assertThat(replay.fencingToken()).isEqualTo(first.fencingToken());   // original token returned
        assertThat(replay.outboxMessageId()).isEqualTo(first.outboxMessageId());
        assertThat(replayVersionOf(scenario)).isEqualTo(1);                  // CAS not re-run
    }

    @Test
    void twoConcurrentReplays_exactlyOneWins() throws Exception {
        UUID tenant = UUID.randomUUID(), ruleSet = UUID.randomUUID(), scenario = UUID.randomUUID();
        seed(tenant, ruleSet, 1, scenario);
        ScenarioReplayOrchestrator orch = newOrchestrator();

        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        // Distinct idempotency keys so both pass the idempotency claim and genuinely race on the CAS.
        Callable<Object> attempt = () -> {
            start.await();
            try {
                return tx.execute(s -> orch.initiateReplay(new ReplayCommand(scenario, tenant, 0, UUID.randomUUID())));
            } catch (RuntimeException e) {
                return e;
            }
        };
        Future<Object> a = pool.submit(attempt);
        Future<Object> b = pool.submit(attempt);
        start.countDown();
        List<Object> results = List.of(a.get(15, TimeUnit.SECONDS), b.get(15, TimeUnit.SECONDS));
        pool.shutdownNow();

        long winners = results.stream().filter(ReplayToken.class::isInstance).count();
        long conflicts = results.stream().filter(ConcurrentReplayException.class::isInstance).count();
        assertThat(winners).as("exactly one replay wins").isEqualTo(1);
        assertThat(conflicts).as("the other is a 409 contention").isEqualTo(1);
        assertThat(replayVersionOf(scenario)).isEqualTo(1);   // version advanced exactly once
    }
}
