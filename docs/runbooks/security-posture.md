# Runbook: SecurityPostureNotHardened

**Alert:** `SecurityPostureNotHardened` · **Severity:** page · **Signal:** `chaosforge_security_hardened{env!="lab"} == 0`

---

## What fired, in one line

A ChaosForge instance **outside the lab** is running on **lab security posture** — at least one control is
off — and it started anyway because `CHAOSFORGE_DEPLOYMENT` was not set to `deployed`.

## What this is NOT

This is not "deployed and unhardened". `DeploymentPostureGuard` makes that state **unreachable**: with
`chaosforge.deployment=deployed`, a missing control throws during context refresh, before the web server
binds, so the process never serves `/actuator/prometheus`. A dead process emits no metrics.

This alert covers the **one gap the fail-fast structurally cannot**: the marker itself being forgotten. A
flag cannot protect against a forgotten flag — this gauge is that cover, and it is the only one.

## Why it is page-level

Depending on the service, "lab posture" means at least one of:

| Service | Control off | Consequence |
|---|---|---|
| control-plane | `chaosforge.mtls.internal-peer-cn` unset | `/internal` falls back to `permitAll`. Its `tenantId` is **peer-asserted and unverified** — any reachable caller reads **any tenant's** rule-set. This is the ADR-0532 cross-tenant read. |
| control-plane / execution-service | `server.ssl.bundle` unset / `client-auth != need` | Intra-service traffic is plain HTTP; no client cert is required. On exec this exposes the OPERATOR-gated kill switch (a global control) without mTLS. |
| execution-service / edge-gateway | `chaosforge.control-plane.base-url` not `https://` | The exec/gateway → CP leg is plaintext, carrying the **forwarded tenant JWT in the clear**. |
| execution-service | `chaosforge.target.allowed-hosts` empty | `TargetUrlGuard` is in open mode on the service that actually fires the faults — an SSRF primitive pointed at whatever the pod can reach (ADR-0534). |

Cross-tenant data exposure is the realistic worst case. Treat as a security incident, not a config nit.

## Diagnose

1. **Find which controls are off.** The instance logged them at startup — it names each gap explicitly:

   ```
   grep "SECURITY POSTURE" <service log>
   # SECURITY POSTURE: lab-only, NOT hardened (deployment=lab): server.ssl.bundle unset — ... |
   #   chaosforge.mtls.internal-peer-cn unset — /internal is permitAll ...
   ```

2. **Confirm which instances.** `chaosforge_security_hardened` is per-job; `{{ $labels.job }}` on the alert
   names the service. A partial posture (one service hardened, another not) is still an incident.

3. **Do not diagnose from the gauge alone** — it is a single 1/0. The startup WARN is the diagnosis.

## Remediate

1. Restart the affected instance with the profile that supplies the controls, and the marker that asserts
   them:

   ```
   --spring.profiles.active=mtls
   CHAOSFORGE_DEPLOYMENT=deployed
   TARGET_ALLOWED_HOSTS=<sanctioned system(s) under test>   # required by the guard when deployed
   ```

2. With `CHAOSFORGE_DEPLOYMENT=deployed`, a still-incomplete posture now **fails startup** with a message
   naming each gap — the loud failure is the intended outcome. Fix what it names; do not set the marker
   back to `lab` to make startup succeed.

3. **Fix the deployment artifact, not just the instance.** A forgotten marker is a property of whatever
   launched the process (compose file / chart / CI). If you only fix the running instance, the next deploy
   reintroduces it.

## Do NOT

- **Do not set `env: lab`** on a real deployment's scrape config to silence this. That is the one action
  that permanently blinds the only cover for this failure mode. `env: lab` belongs solely to
  `docker/prometheus/prometheus.yml`.
- **Do not set `chaosforge.deployment=lab`** to get past a startup failure. That converts a loud, correct
  refusal back into the silent insecure start this whole mechanism exists to eliminate.
- **Do not** "fix" an SSRF-allowlist failure by setting `chaosforge.target.block-private-networks=false`.
  That is explicitly forbidden by `target-validation-rules.md`; the allowlist is the ceiling.

## Known limits of this signal

- **It cannot fire against live lab data, by construction** (the lab stamps `env: lab`). Its firing
  behaviour is therefore proven by promtool unit tests — `docker/prometheus/alerts_test.yml` covers: inert
  on `env=lab`, **fires on an absent `env` label** (the fail-closed property), fires per-job on a partial
  rollout, and honours `for: 5m`. What remains unproven is only the end-to-end path — a real non-lab scrape
  actually reaching a pager. That is C31 game-day territory, and there is no Alertmanager in the lab.
- The promtool tests are a **manual gate**: they need a promtool binary/container and are not wired into
  `./gradlew check`, so nothing stops this rule's annotations drifting out of step with them.
- It is **fail-closed by omission**: a Prometheus with no `env` label matches `env!="lab"` and alerts. That
  is deliberate — a new deployment that forgets everything still pages.
- The gauge is a **boolean roll-up**. It tells you a control is off, never which one. That lives in the
  startup WARN only.
