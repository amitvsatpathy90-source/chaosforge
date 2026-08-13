# Game Day (C31) — staging chaos exercise

**Goal.** Run a controlled chaos game day against a real staging stack and prove the acceptance gate:

> **C31 — one staging game day survived without manual data repair.**

"Survived without manual data repair" has a precise meaning here. During and after the exercise, **no
human may**:

- hand-edit a row (`UPDATE`/`DELETE`/`INSERT` to fix state),
- hand-reset a Kafka offset or hand-replay a message via SQL,
- restart a service to "unstick" data,
- touch the outbox, inbox, `scenario_run`, fences, or idempotency tables to recover.

Every degradation must **self-heal** (the relay drains, the reaper sweeps, the DLQ-retry republishes,
the fence/inbox dedups) or terminate to a **clean, observable terminal state** that is *triage*, not
*repair* — a record sitting in a `.DLQ` for a human to read is allowed; a record that needs a human to
run SQL is a **fail**.

This runbook is the procedure; it is *not* the sign-off. The sign-off is the [attestation](#9-sign-off--c31-attestation) at the end, completed live.

---

## 1. When to run

- Before declaring the platform production-ready (the C31 gate).
- After any change to the replay critical section, the outbox/inbox relays, the DLQ taxonomy, the
  partitioning, or the executor stop-controls.
- On a recurring cadence (quarterly) once in production, as a regression exercise.

**Duration:** ~2–3 hours. **Run in staging only** — never against production or shared infra.

---

## 2. Roles

| Role | Responsibility |
|---|---|
| **Game-day lead** | Drives the script, calls each experiment, makes the abort call |
| **Scribe** | Records start/stop times, observations, metric values, and every assertion's PASS/FAIL in the [log](#8-experiment-log) |
| **Observer(s)** | Watch Grafana + Prometheus alerts + service logs; call out anything unexpected |

One person may hold two roles in a small team, but **lead and scribe must be distinct** so nothing is
lost while the lead is busy injecting a fault.

---

## 3. Prerequisites & access

- A dedicated **staging** stack: Edge Gateway, Control Plane, Execution Service, plus Postgres
  (`chaosforge_cp` + `chaosforge_exec`), Redpanda, Redis, Apicurio, Prometheus, Grafana.
- Shell access to the host running the Compose stack (to `docker pause`/`stop` containers).
- `psql` with `$CP_DB_URL` and `$EXEC_DB_URL` exported (read-only is enough — **writes are the thing
  we are forbidding**).
- A tenant JWT (`$TENANT_JWT`) and an operator JWT (`$OPERATOR_JWT`) for the kill switch.
- Grafana open on the **ChaosForge — SLIs** dashboard; Prometheus open on `/alerts`.
- A **load generator** able to sustain a modest, steady command rate (e.g. a loop issuing
  `POST /v1/scenarios/{id}:run` for a pool of scenarios). Throughput need not be high — it must be
  *continuous* so the SLOs and relays have signal.
- A **synthetic target** with a flippable `/health` endpoint (for the steady-state experiment).

> Companion runbooks (do not duplicate — follow the links when an alert fires):
> [scenario-completion-rate](scenario-completion-rate.md) · [stuck-scenario](stuck-scenario.md) ·
> [dlq-and-outbox](dlq-and-outbox.md) · [kill-switch](kill-switch.md) ·
> [steady-state-abort](steady-state-abort.md).

---

## 4. Abort-the-game-day criteria (safety)

Stop the exercise and begin recovery if **any** of these occur — these mean the game day itself is
causing harm beyond the experiment:

- Staging data loss that is **not** recovering on its own after the fault is removed (a real bug — stop
  and capture state for debugging).
- A fault you cannot reverse (a container won't `unpause`/`start`).
- An experiment leaks beyond staging (an alert routed somewhere real, traffic to a real dependency).

If you abort, that is a **C31 FAIL** for this run — fix the bug, then re-run from a clean stack.

---

## 5. The steady-state hypothesis

Define "healthy" up front. The system holds its steady state when **all** of these are true under the
baseline load (read them off Grafana / Prometheus):

| Signal | Steady-state condition |
|---|---|
| Scenario completion rate | ≥ 95% reach a terminal state within 10 min (`ScenarioCompletionRateBelowSLO` not firing) |
| Stuck-scenario budget | ≤ 1% swept `INCOMPLETE` (`StuckScenarioBudgetExceeded` not firing) |
| Outbox relay | `outbox.oldest_pending_age_seconds` < 60 (CP + exec); `outbox.pending_count` flat |
| Outbox terminal failures | `outbox.dead_count` = 0 |
| DLQ | no hard-poison growth (`HardPoisonInDLQ` not firing) |
| Command accept latency | CP HTTP P99 ≤ 300 ms |
| Consumer lag | `kafka_consumer_fetch_manager_records_lag_max` bounded and recovering |

Each experiment below **breaks** one part of this on purpose, then asserts the system returns to the
steady state **on its own**.

---

## 6. Pre-flight (E0 — baseline)

1. Bring the stack up clean: `docker compose -f docker-compose_chaosforge.yml up -d --wait`.
2. Start the load generator at the baseline rate. Let it run **10 minutes** to fill the SLO windows.
3. Confirm the full steady-state table in §5 holds. **Record the baseline values** in the log.
4. Snapshot the "books" for the no-repair reconciliation at the end:

```bash
psql "$CP_DB_URL"   -c "SELECT status, count(*) FROM outbox GROUP BY status;"
psql "$EXEC_DB_URL" -c "SELECT status, count(*) FROM outbox GROUP BY status;"
psql "$EXEC_DB_URL" -c "SELECT status, count(*) FROM scenario_run GROUP BY status;"
```

If the baseline does not hold, **do not start** — fix staging first.

---

## 7. Experiments

Run them in order. For each: inject → watch the alert fire → remove the fault → **assert self-heal with
zero manual repair** → record PASS/FAIL. Wait for the steady state to fully return between experiments.

### E1 — Control Plane crash mid-replay (publish-side atomicity)

- **Breaks:** the window between the outbox DB commit and the Kafka broker ack.
- **Inject:** under load, `docker kill chaosforge-control-plane` (or the CP container) abruptly; restart
  it after ~20 s.
- **Expect:** rows committed but not yet acked stay `PENDING`; on restart the `OutboxPoller` re-claims
  and publishes them. No duplicates (the consumer inbox dedups on `message_id`), no lost commands.
- **Verify:**
  - `outbox.pending_count` spikes then drains to baseline; `outbox.dead_count` stays 0.
  - Completion rate returns to ≥ 95%.
  - `inbox.duplicates_suppressed_total` may tick up (redelivery) — that is dedup *working*, not a fault.
- **No-repair assertion:** you ran **zero** SQL writes; the relay drained itself. ✅/❌

### E2 — Execution Service crash mid-execution (Phase-2→3 reaper, C16)

- **Breaks:** a run that claimed the inbox (Phase 2) but dies before finalize (Phase 3) — it cannot
  self-heal via redelivery (its `message_id` is already inbox-deduped).
- **Inject:** `docker kill` the Execution Service mid-run; restart after ~20 s.
- **Expect:** the orphaned run sits `IN_PROGRESS`, then the `IncompleteRunSweeper` marks it `INCOMPLETE`
  within the threshold (`chaosforge.run.sweep.incomplete-after-seconds`, default 360 s).
- **Verify:**
  - `chaosforge_run_swept_incomplete_total` increments; `StuckScenarioBudgetExceeded` may fire briefly,
    then clears.
  - `SELECT status, count(*) FROM scenario_run GROUP BY status;` shows the orphan as `INCOMPLETE`, not a
    permanent `IN_PROGRESS`.
- **No-repair assertion:** the sweeper, not a human, marked the run terminal. ✅/❌ (Re-issuing the
  scenario, if business still needs it, is a normal API call — **not** data repair.)

### E3 — Kafka (Redpanda) partition (relay bounded + recovery)

- **Breaks:** the producer→broker path.
- **Inject:** `docker pause chaosforge-redpanda` for ~60 s, then `docker unpause`.
- **Expect:** relay sends abort at the bounded send-timeout (no hung poll threads); rows back off and
  stay `PENDING`; `OutboxRelayLagging` fires. On unpause, the backlog drains; consumer lag recovers.
- **Verify:** `outbox.oldest_pending_age_seconds` climbs past 60 (alert), then returns < 60; no `DEAD`
  rows unless the pause exceeded the full retry budget (it should not); completion rate recovers.
- **No-repair assertion:** the backlog drained on reconnect; no offsets touched. ✅/❌

### E4 — Postgres partition (bounded failure, C19)

- **Breaks:** the executor's DB dependency.
- **Inject:** `docker pause` the **exec** Postgres for ~30 s, then `docker unpause`.
- **Expect:** in-flight DB ops abort at `socketTimeout`/`statement_timeout` (≤ 30 s) — **no** thread
  hangs past `max.poll.interval.ms`, **no** rebalance storm. Affected commands route to the DLQ as
  `INFRA_TRANSIENT` (replayable). On unpause, the **DLQ retry consumer** republishes them with a fresh
  `message_id` and they complete.
- **Verify:** `chaosforge.dlq.routed{dlq_reason="INFRA_TRANSIENT"}` ticks up, then
  `chaosforge.dlq.retry{outcome="republished"}` drains it; completion rate recovers; the consumer group
  does not enter a crash-rebalance loop (check exec logs).
- **No-repair assertion:** the retry consumer, not a human, replayed the transient failures. ✅/❌

### E5 — Poison pill (DLQ taxonomy, C14)

- **Breaks:** decode/validation at pipeline step 1.
- **Inject:** publish a non-Avro / wrong-tenant command to `chaosforge.scenario.commands.v1` (or use a
  staging tool that does). Follow it with a **valid** command on the same partition.
- **Expect:** the poison routes to `.DLQ` with `x-dlq-reason: SCHEMA_INVALID`; the **partition advances**
  (the valid follow-up is processed — no stall). `HardPoisonInDLQ` fires.
- **Verify:**
  ```bash
  kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic chaosforge.scenario.commands.v1.DLQ --from-beginning --property print.headers=true
  ```
  The poison is in the DLQ tagged `SCHEMA_INVALID`; the valid follow-up reached a terminal COMPLETED.
- **No-repair assertion:** the poison is **triage** (human reads the DLQ), not **repair**; the partition
  never stalled. ✅/❌

### E6 — Concurrent replay race (CAS mutex + fencing)

- **Breaks:** two clients replaying the same scenario at the same version.
- **Inject:** `GET /v1/scenarios/{id}` for its `ETag`, then fire **two** `POST /v1/scenarios/{id}:run`
  with the **same** `If-Match` and **different** `Idempotency-Key`s, simultaneously.
- **Expect:** exactly one wins (`202` + new `ETag`); the other gets `409` with a jittered `Retry-After`.
  If the loser is retried, the Execution Service fence drops any stale-version command
  (`FENCING_VIOLATION` → DLQ, not replayed).
- **Verify:** `replay.lock.acquired{result=contended}` (or `replay_conflict_total`) increments;
  `scenario_run` shows exactly one new run for that version — no double effect.
- **No-repair assertion:** the CAS + fence enforced single-execution with no human intervention. ✅/❌

### E7 — Steady-state breach → auto-abort (C20)

- **Breaks:** the steady-state hypothesis — the target goes unhealthy under fault injection.
- **Inject:** run a scenario whose steps target the synthetic target; flip its `/health` to return
  `503` for longer than `max-consecutive-failures × probe interval`.
- **Expect:** the run **auto-aborts** — `ExecutePhase` stops injecting and finalizes `ABORTED`;
  `chaosforge.steady_state.breach_total` increments. Flip `/health` back to `200`; subsequent runs
  proceed normally.
- **Verify:** `SELECT status FROM scenario_run WHERE scenario_id = ...` shows `ABORTED` (terminal); the
  step that would have run after the breach did **not** execute.
- **No-repair assertion:** the experiment stopped itself; the `ABORTED` run is terminal, no repair. ✅/❌

### E8 — Operator kill switch (C19)

- **Breaks:** nothing — this is the *manual* safety control.
- **Inject:** with runs in flight, engage the kill switch:
  ```bash
  curl -X POST https://<exec>:8082/internal/kill-switch \
    -H "Authorization: Bearer $OPERATOR_JWT" -H "Content-Type: application/json" \
    -d '{"reason":"game day E8"}'
  ```
- **Expect:** in-flight scenarios finalize `ABORTED` at their next step boundary; new commands run zero
  steps; `chaosforge.executor.kill_switch.engaged` gauge = 1. Disengage with `DELETE`; runs resume.
- **Verify:** gauge flips 1 → 0; aborted runs are terminal `ABORTED`; after disengage, completion rate
  returns to baseline.
- **No-repair assertion:** engage/disengage left no stuck or half-written runs. ✅/❌

### E9 — Partition-drop purge under load (C28)

- **Breaks:** nothing — verifies retention housekeeping under sustained writes.
- **Inject:** keep the baseline load running; trigger `PartitionMaintenance` (wait for the scheduled run,
  or seed an aged partition in staging and invoke the maintenance) so an aged inbox/outbox day is
  eligible to drop.
- **Expect:** aged day-partitions are **dropped** (O(1)); `chaosforge.partition.dropped_total`
  increments; table/partition size for the dropped day goes to zero; autovacuum is **not** thrashing
  (no rising dead-tuple backlog on the live partitions).
- **Verify:** the dropped partition no longer exists (`to_regclass('inbox_pYYYYMMDD')` is null); current
  traffic is unaffected (completion rate steady throughout).
- **No-repair assertion:** purge happened by partition drop, not a bulk `DELETE` or any manual cleanup. ✅/❌

### E10 — DLQ retry exhaustion (terminal, no silent loss)

- **Breaks:** a target that stays down past the retry budget.
- **Inject:** point a scenario at a target that is **persistently** unreachable; let the DLQ retry
  consumer exhaust `chaosforge.dlq.retry.max-attempts`.
- **Expect:** the record is republished with backoff up to the budget, then dead-lettered terminally as
  `RETRY_EXHAUSTED` (a hard-poison reason the retry consumer never touches again). No infinite loop, no
  silent drop.
- **Verify:** `chaosforge.dlq.retry{outcome="exhausted"}` increments; the record sits in the DLQ tagged
  `RETRY_EXHAUSTED`.
- **No-repair assertion:** the record reached a terminal **triage** state on its own; recovering it later
  is a deliberate operator decision, not data repair. ✅/❌

---

## 8. Experiment log

| # | Experiment | Start | Alert fired? | Self-healed? | Manual writes? (must be 0) | PASS/FAIL |
|---|---|---|---|---|---|---|
| E0 | Baseline | | n/a | n/a | 0 | |
| E1 | CP crash mid-replay | | | | | |
| E2 | Exec crash → reaper | | | | | |
| E3 | Kafka partition | | | | | |
| E4 | Postgres partition | | | | | |
| E5 | Poison pill → DLQ | | | | | |
| E6 | Concurrent replay race | | | | | |
| E7 | Steady-state auto-abort | | | | | |
| E8 | Operator kill switch | | | | | |
| E9 | Partition-drop purge | | | | | |
| E10 | DLQ retry exhaustion | | | | | |

---

## 9. Sign-off — C31 attestation

Complete this **after** the stack has returned to steady state and all alerts have cleared. Re-run the
§6 reconciliation queries and compare to the baseline snapshot.

- [ ] Every experiment E1–E10 is **PASS**.
- [ ] **Zero manual data-repair writes** were performed (the scribe's "Manual writes?" column is all 0).
- [ ] No row is stuck `IN_PROGRESS` past the sweep threshold; none required hand-editing.
- [ ] CP and exec outbox have **no** `PENDING` older than the relay would tolerate, and `DEAD` is only
      what the experiments deliberately created (E10), each explainable.
- [ ] No duplicate business effects (inbox dedup + fencing held; spot-check `scenario_run_log`).
- [ ] Every DLQ record is a **triage** record with a valid `x-dlq-reason`, not a record needing SQL.
- [ ] All SLO alerts (`ScenarioCompletionRateBelowSLO`, `StuckScenarioBudgetExceeded`) returned to OK.
- [ ] Completion rate, outbox lag, and consumer lag are back at baseline.

If every box is checked: **C31 PASS** — record the date, the build/commit SHA, the participants, and
attach the completed experiment log. Update the C31 gate in `architecture specification` with the run date.

If any box is unchecked: **C31 FAIL** — file the gap (the bug or the missing automation), fix it, and
re-run the whole exercise from a clean stack. A partial pass does not count.

---

## Appendix A — fault-injection cheatsheet

| Fault | Command |
|---|---|
| Crash a service | `docker kill chaosforge-<svc>` then `docker start chaosforge-<svc>` |
| Network partition (hang) | `docker pause chaosforge-<svc>` … `docker unpause chaosforge-<svc>` |
| Hard down (refused) | `docker stop chaosforge-<svc>` … `docker start chaosforge-<svc>` |
| Inspect the DLQ | `kafka-console-consumer --bootstrap-server localhost:9092 --topic chaosforge.scenario.commands.v1.DLQ --from-beginning --property print.headers=true` |

Containers: `chaosforge-control-plane`, `chaosforge-execution-service`, `chaosforge-edge-gateway`,
`chaosforge-postgres`, `chaosforge-redpanda`, `chaosforge-redis`, `chaosforge-apicurio`. (Confirm names
with `docker compose ps`.)

## Appendix B — verification query cheatsheet

```bash
# Outbox health (run for both $CP_DB_URL and $EXEC_DB_URL)
psql "$DB" -c "SELECT status, count(*) FROM outbox GROUP BY status;"
psql "$DB" -c "SELECT message_id, attempts, next_attempt_at, created_at
               FROM outbox WHERE status IN ('PENDING','DEAD') ORDER BY created_at LIMIT 20;"

# Run states + any stuck IN_PROGRESS and their age
psql "$EXEC_DB_URL" -c "SELECT status, count(*), max(now()-started_at) AS oldest
                        FROM scenario_run GROUP BY status;"

# Inbox dedup is doing its job (counter, not a row scan)
#   metric: inbox_duplicates_suppressed_total  (via /actuator/prometheus)

# Partition existence (C28)
psql "$EXEC_DB_URL" -c "SELECT to_regclass('inbox_p$(date -u +%Y%m%d)');"
```

Key metrics on `/actuator/prometheus` (and the Grafana SLI dashboard): `outbox.pending_count`,
`outbox.dead_count`, `outbox.oldest_pending_age_seconds`, `chaosforge_scenario_execution_total`,
`chaosforge_run_swept_incomplete_total`, `chaosforge_dlq_routed_total`, `chaosforge_dlq_retry_total`,
`inbox_duplicates_suppressed_total`, `chaosforge.executor.kill_switch.engaged`,
`chaosforge.steady_state.breach_total`, `chaosforge.partition.dropped_total`,
`kafka_consumer_fetch_manager_records_lag_max`.
