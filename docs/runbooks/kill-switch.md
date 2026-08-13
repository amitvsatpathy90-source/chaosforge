# Runbook: executor kill switch (C19)

**What it is.** An in-memory emergency stop in the Execution Service. While engaged, the executor issues
no further fault-injection steps — every in-flight scenario finalizes as `ABORTED` at its next step
boundary, and newly consumed commands run zero steps. `ABORTED` is terminal: it is **not** retried and
**not** dead-lettered (the halt was intentional).

**When to use it.** A chaos experiment is harming a real target (the target's on-call is paging, error
budget is burning, latency is spiking beyond the steady-state hypothesis) and you need fault injection
to stop *now* — faster than disabling scenarios one by one.

## Operate it

The switch is a management endpoint (intra-service mTLS + a re-verified operator JWT — ADR-0524):

```bash
# Engage — halt fault injection
curl -X POST https://<exec-host>:8082/internal/kill-switch \
  -H "Authorization: Bearer $OPERATOR_JWT" -H "Content-Type: application/json" \
  -d '{"reason":"checkout target on-call paged — latency 10x"}'

# Status
curl https://<exec-host>:8082/internal/kill-switch -H "Authorization: Bearer $OPERATOR_JWT"

# Disengage — resume (only once the target is healthy and the experiment is understood)
curl -X DELETE https://<exec-host>:8082/internal/kill-switch -H "Authorization: Bearer $OPERATOR_JWT"
```

Confirm on the dashboard: the `chaosforge.executor.kill_switch.engaged` gauge reads **1** while engaged.

## Why it works during an incident

The switch is a single in-memory flag — `isEngaged()` is one volatile read, no DB or broker round-trip.
That is deliberate: an outage of Postgres or a Kafka partition is *exactly* when you most need to halt,
and a persisted flag would be unreachable then. Proven by `ExecutorControlsUnderFaultIT`
(`killSwitch_abortsExecution_withPostgresStopped`) and the broker-free `KillSwitch`/`ExecutePhase`
unit tests.

## Companion bound: the in-executor deadline

Independently of the switch, each execution is bounded by the aggregate step deadline
(`chaosforge.step.aggregate-timeout-ms`, default 4m < the 5m `max.poll.interval.ms`) and by the DB
timeouts (`socketTimeout`/`statement_timeout` in `application.yml`, ≤30s). So a slow or **partitioned**
dependency cannot stall a consumer thread into a rebalance storm — it surfaces as a bounded
`STEP_TIMEOUT` (→ DLQ, replayable) or `INFRA_TRANSIENT`. Verified by `ExecutePhaseControlsTest` and the
partition tests in `ExecutorControlsUnderFaultIT` / `ExecOutboxRelayIT`.

## Lab limitation

Kill-switch state is **per-instance**. With more than one Execution Service replica, engage on every
replica (or front the fleet with a shared, polled flag) — the executor does not yet share this state.
