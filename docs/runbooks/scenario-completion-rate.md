# Runbook: `ScenarioCompletionRateBelowSLO` (C27)

**Severity:** page · **SLO:** ≥ 95% of accepted commands reach a terminal state within 10 min.

```
completion_rate(10m) =
  sum(rate(chaosforge_scenario_execution_total{job="execution-service"}[10m]))
  / ( that + sum(rate(chaosforge_run_swept_incomplete_total{job="execution-service"}[10m])) )
```

The alert fires when this drops below `0.95` for 10 minutes.

## What it means

Accepted commands are **not finishing**. Because every finished command reaches a terminal outcome
(`success | deduped | dlq-routed`), a falling completion rate means runs are crashing in the
**Phase-2→3 window** — after the inbox claim commits but before finalize — and being swept to
`INCOMPLETE` faster than real completions accrue. This is the genuine availability signal; request
rate alone would not show it.

## First 60 seconds — confirm and scope

1. Open the **Scenario execution outcomes (Exec)** and look at the split. Is `success` falling, or is
   `dlq-routed` spiking, or is the sweeper (`chaosforge_run_swept_incomplete_total`) climbing?
2. Check whether this is correlated with `StuckScenarioBudgetExceeded` (same root cause — see
   [stuck-scenario.md](stuck-scenario.md)) or with `HardPoisonInDLQ` (commands failing semantically).

## Diagnosis by branch

### A. Sweeper climbing → crashes before finalize (most common)
The execution service is dying or losing its DB/Kafka connection mid-run. Confirm:

```bash
# How many runs are stuck vs terminal right now?
psql "$EXEC_DB_URL" -c "SELECT status, count(*) FROM scenario_run GROUP BY status;"
```

- A growing `IN_PROGRESS` count older than the sweep threshold → the service is crash-looping or a
  downstream (Postgres / Kafka) is flapping. Check the execution-service logs for restarts and the
  `kafka_consumer_fetch_manager_records_lag_max` panel for rebalance churn.
- Follow [stuck-scenario.md](stuck-scenario.md) for the remediation; this alert clears once the crash
  source is removed and the next 10m window fills with completions.

### B. `dlq-routed` spiking → commands failing, not stalling
Runs ARE reaching a terminal state, but the *failing* kind. Completion-rate counts dlq-routed as
terminal, so a pure DLQ spike should **not** by itself drop completion rate below 95% — if it has,
you have BOTH a DLQ spike and a stuck-run climb. Triage the DLQ:

```bash
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic chaosforge.scenario.commands.v1.DLQ --from-beginning --property print.headers=true
```

Group by `x-dlq-reason`. `STEP_FAILED` = scenario logic; `INFRA_TRANSIENT`/`STEP_TIMEOUT` = a target
is down (the retry consumer will republish these). See [dlq-and-outbox.md](dlq-and-outbox.md).

### C. No traffic / NaN
If accepted-command rate is ~0, the expression is NaN and the alert does **not** fire. If you are
paged with near-zero traffic, suspect the scrape target is down — check
`up{job="execution-service"}` and that `bootRun` is alive on `:8082`.

## Remediation

- **Crash source (branch A):** restore the failing dependency (Postgres/Kafka), let the service come
  back, and let the `IncompleteRunSweeper` (runs every `chaosforge.run.sweep.interval-ms`, default 60s)
  mark the orphans `INCOMPLETE`. They will not re-run automatically (inbox-deduped); re-issue the
  affected replays from the Control Plane if the scenario still needs to run.
- **Semantic failures (branch B):** fix the scenario/rule-set; `STEP_FAILED` is not replayable.
- Do **not** manually flip `scenario_run` rows — the sweeper is the only writer of `INCOMPLETE` and the
  C16 test depends on that invariant.

## Escalation

If the crash source is not obvious within 15 min, or the stuck count keeps climbing after the
dependency is restored, escalate to the execution-service owner with: the `scenario_run` status
histogram above, the last 200 execution-service log lines, and the `trace_id` of one stuck run.

## Related
- [stuck-scenario.md](stuck-scenario.md) — the budget alert for the same underlying failure.
- [dlq-and-outbox.md](dlq-and-outbox.md) — DLQ triage and outbox health.
