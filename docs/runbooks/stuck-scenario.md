# Runbook: `StuckScenarioBudgetExceeded` (C26)

**Severity:** page · **SLO:** ≤ 1% of scenarios without a terminal event after 15 min.

```
stuck_rate(15m) =
  sum(rate(chaosforge_run_swept_incomplete_total{job="execution-service"}[15m]))
  / ( sum(rate(chaosforge_scenario_execution_total{job="execution-service"}[15m])) + that )
```

Fires when there is real sweep activity **and** `stuck_rate > 0.01`, sustained 5 min.

## What it means

The `IncompleteRunSweeper` is actively marking runs `INCOMPLETE`. Every such run got past the inbox
claim (Phase 2) but never reached finalize (Phase 3), then sat `IN_PROGRESS` past the sweep threshold
(`chaosforge.run.sweep.incomplete-after-seconds`, default **360s** = `max.poll.interval.ms` 5m + 60s
grace). These runs **cannot self-heal**: their `message_id` is already in the inbox, so a Kafka
redelivery is ack-skipped (deliberate — `RunLogDao.markStaleRunsIncomplete`). The sweep turns an
invisible `IN_PROGRESS`-forever inconsistency into a bounded, terminal, observable `INCOMPLETE`.

> Threshold note: the **sweeper** acts at 6 min (just past the poll budget — it must, to avoid sweeping
> a run still legitimately executing). The **SLO** speaks of 15 min. They are intentionally different:
> the sweeper is the operational safety net; the SLO is the business target. A run swept at 6 min is
> already a stuck run by the 15-min SLO, so the sweep counter is the correct early signal.

## First 60 seconds

```bash
# What is stuck, and how stale?
psql "$EXEC_DB_URL" -c "
  SELECT status, count(*),
         min(now() - started_at) AS youngest,
         max(now() - started_at) AS oldest
  FROM scenario_run GROUP BY status ORDER BY status;"
```

A rising `IN_PROGRESS` cohort older than ~6 min, or a climbing `INCOMPLETE` count, confirms it.

## Likely causes

| Cause | Tell | Where to look |
|---|---|---|
| Execution-service crash-looping | restarts in logs; consumer group rebalancing | exec logs; `kafka_consumer_fetch_manager_records_lag_max` |
| Postgres unavailable mid-run | finalize tx fails after the claim committed | exec logs (`SQLException` after claim); DB health |
| A step hangs near the aggregate deadline | runs end as `STEP_TIMEOUT` *or* crash right at the boundary | `chaosforge_scenario_execution_total{outcome="dlq-routed",dlq_reason="STEP_TIMEOUT"}` |
| Sweep threshold mis-set below the poll budget | *fresh* runs wrongly swept | `chaosforge.run.sweep.incomplete-after-seconds` must exceed `max.poll.interval.ms` |

## Remediation

1. **Stop the bleeding:** restore the dependency that is crashing the finalize path (most often
   Postgres or the broker). Once the service is healthy, new runs finalize normally and the 15m rate
   decays below 1%.
2. **Account for the orphans:** the sweeper will mark the stuck cohort `INCOMPLETE` on its next pass
   (every `chaosforge.run.sweep.interval-ms`, default 60s). These runs are terminal-but-failed — they
   did **not** execute their steps to completion. If a scenario still needs to run, **re-issue the
   replay** from the Control Plane (`POST /scenarios/{id}:run` with a fresh `Idempotency-Key`); do not
   expect the original message to redeliver.
3. **If fresh runs are being swept:** the threshold is too low. Raise
   `chaosforge.run.sweep.incomplete-after-seconds` back above `max.poll.interval.ms` and redeploy. This
   is a config bug, not a data problem.

## Do NOT
- Manually `UPDATE scenario_run SET status=...` — the sweeper is the sole writer of `INCOMPLETE`
  (C16 invariant). Hand-editing masks the signal and breaks the safety-net test's assumptions.
- Republish stuck commands with their original `message_id` — the inbox will dedup them to a no-op.
  Replays go through the Control Plane, which mints a fresh command (ADR-0529).

## Escalation
If `IN_PROGRESS`-age keeps climbing after the dependency is restored, escalate to the
execution-service owner with the status/age histogram and one stuck run's `trace_id`.

## Related
- [scenario-completion-rate.md](scenario-completion-rate.md) — the completion-rate SLO; same root cause.
