# Runbook: gateway rate-limit fail-open, token fallback & tenant-policy feed

**What these cover.** Three Edge Gateway rate-limit and policy degradation modes with
distinct behaviors. Tenant-policy loading can fall back to the default policy; the tenant
rate limiter fails open on request-path errors; and the operate-token limiter falls back
to local, per-pod enforcement when its Redis call fails, times out, or its circuit breaker
is open. These behaviors have distinct metric signals and alert rules (ADR-0536, ADR-0537,
ADR-0540, ADR-0543 Amendment 2).

| Alert | Fires when | Means |
|---|---|---|
| `TenantPolicyFeedDown` | Fallback policy-load rate exceeds 50% over 5m, sustained for 10m | More than half of policy loads are falling back to the hardcoded default (600/min), so affected tenants may not get their configured limits. Check the CP policy endpoint and mTLS/CN configuration. |
| `RateLimitFailingOpen` | `chaosforge.gateway.rate_limit{outcome="fail_open"}` rate > 0 for 5m | An error escaped the tenant policy-cache or Redis-enforcement path, allowing affected requests through unthrottled. Control Plane policy-fetch failures normally fall back to the default policy and are tracked separately by `TenantPolicyFeedDown`. |
| `RateLimitTokenLimiterDegraded` | `chaosforge.gateway.rate_limit{outcome="local_fallback", limiter="token"}` rate > 0 for 5m | The operate-token limiter is enforcing a local per-pod limit instead of a globally shared Redis limit. |

## Signals

```
chaosforge.gateway.policy_load{outcome="success"|"fallback"}   # Counter — ControlPlaneClient policy-load outcomes
chaosforge.gateway.rate_limit{outcome, limiter}                # Counter — per-request limiter decisions
# tenant: allowed | rate_limited | fail_open
# token:  rate_limited | local_fallback
```

Structured WARN logs on the failing path:
- `tenant policy load failed (<ExceptionClass>) — fail-open default for tenant …<last4>` (`ControlPlaneClient`)
- `rate-limit fail-open (<ExceptionClass>) — request allowed unthrottled for tenant …<last4>` (`RateLimitWebFilter`)
- `rate-limit Redis unavailable (<ExceptionClass>) — operate token on local per-pod limit, tenant …<last4>` (`RateLimitWebFilter`)

## When `TenantPolicyFeedDown` fires

1. **Check CP reachability from the gateway.** This is an mTLS-internal call
   (`GET /internal/tenants/{id}/policy`, ADR-0532) — confirm CP is up and the gateway's client
   cert CN matches `chaosforge.mtls.internal-gateway-cn` on CP. A CN mismatch is a **403**, which
   looks identical to an outage from the gateway's side (both count as `fallback`).
2. **Check the `cache-load` circuit breaker** (`ControlPlaneClient`) — if CP is flapping, the CB may
   be open and short-circuiting every load. Confirm with CP's own health/logs, not just the gateway.
3. **Impact while this is firing:** more than half of policy-load activity is falling
   back to the 600/min default. Affected tenants provisioned for less are
   under-throttled (cost/blast-radius exposure); those provisioned for more are
   over-throttled (false 429s).
4. **This does not disable rate limiting** — the sliding window still runs, just against the wrong
   number. Contrast with `RateLimitFailingOpen` below: the tenant-specific rate limit is not
   enforced for affected requests, although operate-scoped `/mcp` requests may still be
   constrained by the separate token limiter.

## When `RateLimitFailingOpen` fires

1. **Check the failing dependency.** Confirm that the Gateway can reach its rate-limit
   Redis instance (single-instance in the lab; no cluster). If Redis is healthy, inspect
   the tenant-policy lookup path and the `cache-load` circuit breaker for Control Plane
   connectivity or policy-loading errors.

2. **Understand the impact.** The tenant limiter has allowed affected requests without
   enforcing the tenant-specific rate limit. The impact depends on the request path:
    - `/v1` requests bypass the operate-token limiter. `/mcp` requests use it when the
      credential has `SCOPE_chaosforge.operate`, even for reads or polling. A credential
      with only `SCOPE_chaosforge.read` bypasses the token limiter.
    - If the token limiter's Redis call also fails, times out, or its circuit breaker is
      open, it falls back to local per-pod enforcement. That is not a globally shared quota.
    - If the problem is limited to tenant-policy lookup while Redis remains healthy, the
      operate-token limiter can continue using its Redis-backed limit.

3. **Choose the response.** Fail-open is the intended availability-over-rate-limit-
   correctness trade-off, not proof that rate limiting is healthy. If Redis is expected
   to recover shortly, monitor the dependency and the signals. For an extended outage,
   assess whether upstream service capacity can absorb the additional traffic or whether
   another load-shedding mechanism is needed. This project has no fail-closed mode for
   the tenant limiter; introducing one would require its own ADR.

4. **Correlate the alerts.** If `RateLimitTokenLimiterDegraded` and
   `RateLimitFailingOpen` fire together, investigate the shared Redis dependency and
   `gateway-ratelimit-redis` circuit breaker first. If `TenantPolicyFeedDown` also fires,
   investigate the Control Plane policy endpoint and its mTLS/network path separately.

## When `RateLimitTokenLimiterDegraded` fires

1. **Check Redis first.** Confirm the rate-limit Redis instance is reachable from the
   gateway (single-instance in the lab; no cluster). If Redis is healthy, inspect the
   `rate-limit fail-open` warning and exception class to identify the error that propagated.
   Check `policy_load{outcome="fallback"}` and `TenantPolicyFeedDown` separately for
   Control Plane policy-fetch failures.

2. **Understand the quota limitation.** Operate-token requests are still rate-limited,
   but each Gateway pod maintains its own local fixed-window counter. The counter starts
   independently of the Redis quota; it does not continue the globally shared count.
   With multiple pods, aggregate allowance can grow with pod count. The fixed-window
   boundary can admit up to twice the local limit, and Caffeine eviction can reset a
   token's local count. This is best-effort local enforcement, not a strict global quota.

3. **Confirm the fallback signal.** Query the `local_fallback` counter for
   `limiter="token"`. An increasing count confirms that operate-scoped `/mcp` requests
   are using the fallback. A flat count doesn't prove recovery — there may be no
   operate-scoped traffic.

4. **Check tenant-limit impact too.** Both limiters use the
   `gateway-ratelimit-redis` circuit breaker. Redis failures can affect both paths:
   the token limiter falls back to local enforcement, while the tenant limiter fails
   open. Check `RateLimitFailingOpen` and the `fail_open` counter tagged
   `limiter="tenant"`.

5. **Verify recovery.** Once Redis is reachable and the circuit breaker recovers, new
   token decisions should return to Redis-backed enforcement.

## Verification after recovery

```bash
curl -s http://localhost:9090/api/v1/query --data-urlencode \
  'query=sum(rate(chaosforge_gateway_rate_limit_total{job="edge-gateway",outcome="local_fallback",limiter="token"}[5m])) or vector(0)'
# expect 0 after a full 5m window without new token fallback events

curl -s http://localhost:9090/api/v1/query --data-urlencode \
  'query=sum(rate(chaosforge_gateway_rate_limit_total{job="edge-gateway",outcome="fail_open",limiter="tenant"}[5m])) or vector(0)'
# expect 0 after a full 5m window without new tenant fail-open events
```

`TenantPolicyFeedDown` uses `for: 10m`; `RateLimitFailingOpen` and
`RateLimitTokenLimiterDegraded` use `for: 5m`. These windows require sustained
degradation before the corresponding alerts fire.
