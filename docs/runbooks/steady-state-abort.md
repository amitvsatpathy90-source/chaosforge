# Runbook: steady-state auto-abort (C20)

**What it is.** The executor continuously tests a **steady-state hypothesis** during fault injection —
"the target stays healthy while we inject faults" — by probing the target's health endpoint between
steps. If health fails for `max-consecutive-failures` probes in a row, the blast radius has exceeded
the hypothesis and the run **auto-aborts** (finalized `ABORTED`, terminal — not retried, not
dead-lettered). This is the experiment pulling its own stop; the operator [kill switch](kill-switch.md)
is the manual equivalent.

## Configuration (`application.yml`)

```yaml
chaosforge:
  steady-state:
    enabled: true
    max-consecutive-failures: 3   # tolerate transient blips; abort only on sustained degradation
    health-path: /health          # appended to the target's <scheme>://<authority>
```

The probe target is derived from the scenario's first step target: `http://checkout.svc/api/pay` →
probes `http://checkout.svc/health`. A scenario whose target has no parseable origin is not probed
(the check is inert) — it falls back to the kill switch and the aggregate deadline.

## Signals

- `chaosforge.steady_state.breach_total` (counter) — increments once per auto-abort. **Any** increase
  means an experiment harmed its target enough to self-abort; investigate the target, not the executor.
- Aborted runs finalize as `scenario_run.status = 'ABORTED'` with a result event on the outbox.

## When it fires — what to do

1. **Confirm the target recovered.** The abort stopped *new* faults; in-flight effects may linger.
   Check the real target's health and error budget.
2. **Read the breach context** from the structured log: `steady-state breach at <healthUrl> —
   auto-aborting scenario <id>` carries the `trace_id` / `scenario_id` (tenant masked).
3. **Decide on the experiment.** A breach is usually a *finding*, not a bug — the experiment found a
   fragility. Fix the target (or narrow the scenario's blast radius) before re-running. Re-issue the
   replay from the Control Plane when ready (a fresh `Idempotency-Key`); the aborted run is terminal.
4. **If breaches are frequent across scenarios,** the target (or a shared dependency) is unhealthy
   independent of the experiments — escalate to the target owner; consider engaging the global
   [kill switch](kill-switch.md) until it stabilizes.

## Tuning

- **Too sensitive** (aborting on blips): raise `max-consecutive-failures`, or point `health-path` at a
  deep health check rather than a shallow liveness ping.
- **Too lax** (target harmed before aborting): lower `max-consecutive-failures`, or shorten the target
  RestClient read timeout so a slow health endpoint counts as unhealthy sooner.

## Lab limitations

- The hypothesis is a **consecutive-health-failure** count, probed between steps — not a latency- or
  error-rate-percentile SLO. A richer hypothesis (p99 latency, error rate over a window) is the
  production extension.
- Probing happens at step boundaries, so a scenario with very few steps may finish before a sustained
  breach is detected; the aggregate deadline and per-step timeouts still bound it.
- A breach aborts **only the offending run** (scenario-scoped). Escalating a breach to the global kill
  switch (halt every scenario hitting a shared target) is a deliberate, documented non-default.
