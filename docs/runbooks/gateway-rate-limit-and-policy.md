# Runbook: gateway rate-limit fail-open & tenant-policy feed (arch-audit H1/H3)

**What these cover.** Two related Edge Gateway degradation modes, both **fail-open by design**
(architecture specification: availability over rate-limit correctness during a dependency outage) but both now
instrumented so the degradation is visible instead of silent (ADR-0536, ADR-0537).

| Alert | Fires when | Means |
|---|---|---|
| `TenantPolicyFeedDown` | >50% of `chaosforge.gateway.policy_load` loads are `outcome="fallback"` for 10m | The gateway can't reach CP's `/internal/tenants/{id}/policy` — tenants are getting the hardcoded default (600/min) instead of their configured limit. |
| `RateLimitFailingOpen` | `chaosforge.gateway.rate_limit{outcome="fail_open"}` rate > 0 for 5m | Redis (or the policy lookup) is erroring on the request path — rate limiting is not being enforced at all for affected requests. |

## Signals

```
chaosforge.gateway.policy_load{outcome="success"|"fallback"}   # Counter — policy cache loader (RateLimitWebFilter's TenantPolicyCache)
chaosforge.gateway.rate_limit{outcome="allowed"|"rate_limited"|"fail_open"}   # Counter — per-request decision
```

Structured WARN logs on the failing path:
- `tenant policy load failed (<ExceptionClass>) — fail-open default for tenant …<last4>` (`ControlPlaneClient`)
- `rate-limit fail-open (<ExceptionClass>) — request allowed unthrottled for tenant …<last4>` (`RateLimitWebFilter`)

## When `TenantPolicyFeedDown` fires

1. **Check CP reachability from the gateway.** This is an mTLS-internal call
   (`GET /internal/tenants/{id}/policy`, ADR-0532) — confirm CP is up and the gateway's client
   cert CN matches `chaosforge.mtls.internal-gateway-cn` on CP. A CN mismatch is a **403**, which
   looks identical to an outage from the gateway's side (both count as `fallback`).
2. **Check the `cache-load` circuit breaker** (`ControlPlaneClient`) — if CP is flapping, the CB may
   be open and short-circuiting every load. Confirm with CP's own health/logs, not just the gateway.
3. **Impact while this is firing:** every tenant is getting the 600/min default regardless of their
   configured limit — a tenant provisioned for less is under-throttled (cost/blast-radius exposure);
   a tenant provisioned for more is over-throttled (false 429s).
4. **This does not disable rate limiting** — the sliding window still runs, just against the wrong
   number. Contrast with `RateLimitFailingOpen` below, where limiting is off entirely.

## When `RateLimitFailingOpen` fires

1. **Check Redis.** This is the far more common trigger than a policy-lookup failure — confirm the
   rate-limit Redis instance (single-instance in the lab; no cluster — see Known Limitations) is up
   and reachable from the gateway.
2. **Impact while this is firing:** requests are passing through completely unthrottled. This is the
   intended fail-open behavior (protecting gateway availability over rate-limit correctness), not a
   bug — but it needs a human decision, not silence:
   - If Redis is down and expected to recover shortly, monitor and let it fail open.
   - If Redis is down for an extended period, consider whether upstream (CP) capacity can absorb
     unthrottled traffic, or whether to shed load by another means (this project has no fail-closed
     mode for rate limiting — that tradeoff would need its own ADR).
3. **Both signals firing together** usually means the shared dependency path (mTLS/network to CP,
   or the box Redis lives on) is the actual root cause, not two independent failures.

## Verification after recovery

```bash
curl -s http://localhost:9090/api/v1/query --data-urlencode \
  'query=rate(chaosforge_gateway_rate_limit_total{outcome="fail_open"}[5m])'
# expect 0 once Redis/policy-feed recovery is confirmed
```

Both alerts are `for:`-windowed (10m / 5m) specifically so a single transient blip doesn't page —
only sustained degradation does.
