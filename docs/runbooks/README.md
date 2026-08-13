# ChaosForge Runbooks

Operational response procedures for the alert rules in
[`docker/prometheus/alerts.yml`](../../docker/prometheus/alerts.yml). Each firing alert carries a
`runbook` annotation pointing at the file here that tells you what to do.

## Gated SLO alerts (acceptance gate C26/C27)

| Alert | SLO | Runbook |
|---|---|---|
| `ScenarioCompletionRateBelowSLO` | ≥ 95% of accepted commands reach a terminal state within 10 min (C27) | [scenario-completion-rate.md](scenario-completion-rate.md) |
| `StuckScenarioBudgetExceeded` | ≤ 1% of scenarios without a terminal event after 15 min (C26) | [stuck-scenario.md](stuck-scenario.md) |

## Operational backstops

| Alert | Signal | Runbook |
|---|---|---|
| `HardPoisonInDLQ` | non-replayable record landed in the DLQ | [dlq-and-outbox.md](dlq-and-outbox.md) |
| `OutboxDeadLetterPresent` | an outbox row exhausted MAX_ATTEMPTS → DEAD (CP or exec relay) | [dlq-and-outbox.md](dlq-and-outbox.md) |
| `OutboxRelayLagging` | oldest PENDING outbox row > 60s old (CP or exec relay) | [dlq-and-outbox.md](dlq-and-outbox.md) |
| `PartitionDefaultRowsPresent` | a row landed in an un-droppable `_default` partition (C28) | [dlq-and-outbox.md](dlq-and-outbox.md) |
| `OutboxStragglerBacklog` | PENDING rows older than the hot-lane claim window, sustained | [dlq-and-outbox.md](dlq-and-outbox.md) |
| `CommandAcceptLatencyP99High` | CP HTTP P99 > 300ms (command-accept SLO), excludes AI authoring | [dlq-and-outbox.md](dlq-and-outbox.md) |
| `TenantPolicyFeedDown` | gateway tenant-policy loads mostly falling back to default | [gateway-rate-limit-and-policy.md](gateway-rate-limit-and-policy.md) |
| `RateLimitFailingOpen` | gateway rate limiting failing open (Redis/policy error) | [gateway-rate-limit-and-policy.md](gateway-rate-limit-and-policy.md) |

## Operator controls & safety

| Control | Use | Doc |
|---|---|---|
| Executor kill switch | manual emergency stop — halt all fault injection now | [kill-switch.md](kill-switch.md) |
| Steady-state auto-abort | automatic per-run stop when the target breaches the hypothesis | [steady-state-abort.md](steady-state-abort.md) |

## Exercises

| Exercise | Purpose | Doc |
|---|---|---|
| Game day (C31) | staging chaos exercise — prove the system survives fault injection with no manual data repair | [game-day-c31.md](game-day-c31.md) |

## Severity conventions

- **`page`** — respond now: either a gated SLO is actively breaching (both SLO alerts), or the backstop
  covers damage that cannot self-heal (`PartitionDefaultRowsPresent` — an un-droppable partition row) or
  a security/isolation control going dark (`TenantPolicyFeedDown` — per-tenant limits silently disabled).
- **`ticket`** — a backstop tripped; investigate within the working day. It is not (yet) an SLO breach
  but left alone it becomes one.

## How the signals are computed

Every command that finishes reaches exactly one **terminal** outcome —
`success | deduped | dlq-routed` — each of which increments
`chaosforge_scenario_execution_total`. The only non-terminal fate is a crash in the Phase-2→3 window
(after the inbox claim commits, before finalize); such a run cannot self-heal (its `message_id` is
already inbox-deduped, so Kafka redelivery is ack-skipped) and is caught by the `IncompleteRunSweeper`,
which marks it `INCOMPLETE` and increments `chaosforge_run_swept_incomplete_total`. So:

```
completion_rate = terminal / (terminal + stuck)
stuck_rate      = stuck    / (terminal + stuck)
```

are computed from those two series alone — no `tenant_id` label anywhere (cardinality rule). Tenant
detail for a specific incident lives on **traces and structured logs**, keyed by `trace_id` /
`scenario_id` (tenant masked to last 4).

## Lab limitation — no Alertmanager

This lab Prometheus loads and **evaluates** these rules (they go `PENDING` → `FIRING`, visible on
`http://localhost:9090/alerts` and queryable as the `ALERTS` series), but no Alertmanager is wired, so
nothing is *routed* to a pager/Slack in the lab. Wiring Alertmanager (routing, grouping, silences) is
the production add-on; the rule definitions and these runbooks are the portable part. Grafana can also
alert on the same expressions against the `chaosforge-prometheus` datasource.
