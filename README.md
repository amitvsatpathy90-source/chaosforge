# ChaosForge (CF)

A multi-tenant chaos-engineering / API-resilience control plane, built as a **proof-of-capability
artifact**. The point is not the feature set — it is that every hard distributed-systems decision is
*argued* (in an ADR), *enforced* (in code and CI), and *demonstrated* (by a test that runs against
real infrastructure).

> **Lab benchmarks only — never production claims.** See [Lab Benchmark Disclosure](#lab-benchmark-disclosure).

Java 21 · Spring Boot 4.1.x · Gradle (Kotlin DSL) · Postgres · Redis · Kafka (Redpanda) · Apicurio ·
Resilience4j · Micrometer/Prometheus/Grafana · Spring AI (Ollama).

---

## The five claims

Each claim is a thing teams routinely get *subtly wrong*. ChaosForge takes a position, enforces it,
and proves it.

| # | Claim | Where it lives | Proven by |
|---|---|---|---|
| 1 | **Reactive when justified, imperative otherwise** — not reactive-everything cargo-culting | Gateway = WebFlux; CP + Exec = MVC + virtual threads (ADR-0500) | `GatewayRulesTest` (no `.block()`), `*ArchTest` (no `Mono`/`Flux` in CP/Exec) |
| 2 | **Publish-side atomicity** — crash recovery without duplicates | Transactional outbox + consumer inbox, SENT-flip gated on broker ack | `OutboxPollerCrashIT`, `ClaimPhaseIT` (inbox dedup) |
| 3 | **Distributed mutex done correctly** — a fencing token, not a lock you hope holds | CAS on `scenario_replay_state.replay_version`; Redlock *rejected with reasons* (ADR-0502 → ADR-0522 → **ADR-0528**) | `ReplayCriticalSectionIT` (incl. a real 2-thread race) |
| 4 | **The cost of caching is understood** — explicit invalidation, documented staleness | Two-level cache (Caffeine L1 + Redis L2), single-flight, TTL backstop | `TwoLevelCacheIT` (100 cold reads → 1 load) |
| 5 | **AI held to the same discipline** — the LLM is an authoring tool, never on the replay path | Spring AI in CP only; determinism boundary enforced (ADR-0518) | `ExecutionServiceArchTest` (no Spring AI in Exec), `@Valid` + tenant-target gate |

---

## Architecture

```
Client ──JWT──▶ Edge Gateway  (WebFlux, :8080)
                  · Redis Lua sliding-window rate limit — true global, fail-open
                  · L1 tenant-policy cache (Caffeine)
                  · WebClient → CP, wrapped: bulkhead → CB → timeout(3s)
                  · forwards Authorization + If-Match + Idempotency-Key intact
                        │
                        ▼
            Control Plane  (MVC + virtual threads, :8081)
                  · CRUD: Tenant / Scenario / RuleSet (append-only)
                  · Replay critical section — ownership-first CAS (ADR-0528) ⬇
                  · Transactional outbox → Kafka (SKIP LOCKED, lease-on-claim, UUIDv7)
                  · AI authoring → Ollama (BeanOutputConverter + @Valid gate)
                        │ Avro binary on chaosforge.scenario.commands.v1
                        ▼
            Execution Service  (MVC + VT + @KafkaListener, :8082)
                  1. Avro decode + tenant verify   → SCHEMA_INVALID → DLQ
                  2. Fencing (x-replay-version)     → FENCING_VIOLATION → DLQ
                  3. Inbox dedup (ON CONFLICT)      ── fencing PRECEDES inbox (load-bearing)
                  4. Rule-set load by PINNED (id, version) — never latest
                  5. Step exec (Resilience4j)       → INFRA_TRANSIENT / STEP_TIMEOUT / STEP_FAILED
                  6. Result via outbox
                  7. Manual ack (post-commit only)
```

### Runtime surface split (deliberate; do not collapse)

| Service | Runtime | Hard constraint |
|---|---|---|
| Edge Gateway | WebFlux (Netty) | No `.block()`. Reactor Context for `tenant_id`. ReactiveResilience4j CB on every upstream call. |
| Control Plane | MVC + virtual threads | No `Mono`/`Flux`. CAS replay (no advisory lock). JDBC on the request VT (no `jdbcExecutor`), bounded by the Hikari connection pool; measured pinning == 0. |
| Execution Service | MVC + VT + Kafka | `@KafkaListener` returns `void`. Manual ack post-commit. Fencing-before-inbox ordering. |

---

## The replay critical section (ADR-0528) — the headline

`POST /scenarios/{id}:run` is the highest-contention, highest-security write in the system. It runs
**five SQL statements, strictly ordered, in one transaction**, preceded by
`SET LOCAL lock_timeout='750ms' < statement_timeout='5s'`:

1. **Ownership + pin** — tenant-scoped read of the rule-set tuple. Empty → **404** (a cross-tenant or
   absent scenario is *indistinguishable*, and the CAS is never reached — no version oracle, no write).
2. **Idempotency claim** — `INSERT … ON CONFLICT DO NOTHING`. A completed key returns the original
   token (**200**); an in-flight duplicate → **409**.
3. **CAS bump** — `UPDATE … WHERE replay_version = :expectedVersion RETURNING replay_version`.
   0 rows → **409** (concurrent replay or stale `If-Match`). The new version *is* the fencing token.
4. **Outbox insert** — PENDING, Avro payload, fencing headers, UUIDv7 `message_id`.
5. **Idempotency complete** — store the token so a lost-response retry is safe.

Why it is correct, not just plausible:

- `expectedVersion` **must** come from a prior `GET` (the `ETag` / `If-Match`). Read-then-CAS in one
  transaction makes the predicate trivially true and the 409 path can never fire — this is *forbidden*
  and ArchUnit-adjacent guarded.
- `lock_timeout < statement_timeout` makes contention a fast **409**, not a 5-second 500.
- Ownership precedes the CAS, so `{absent, cross-tenant}` collapse into one 404 with zero side effect.

`ReplayCriticalSectionIT` proves all of this — including `twoConcurrentReplays_exactlyOneWins`, a real
two-thread race on real Postgres row locks.

---

## Running it

Requires Docker (Testcontainers + the local stack) and a JDK 21 toolchain (Gradle provisions it).

```bash
# Ensure .env exists and source variables into shell
cp -n .env.example .env || true
set -a; source .env; set +a

# 0. Infrastructure & Environment Setup
docker compose -f docker-compose_chaosforge.yml up -d
docker/jwks/generate-jwks.sh

EDGE_GATEWAY_URL="http://localhost:8080"
EXEC_DB_NAME=$(echo "$EXEC_DB_URL" | sed 's/.*\///')

# Start Services in separate terminals (Source .env in each tab before bootRun):
# Tab 1: set -a; source .env; set +a && ./gradlew :edge-gateway:bootRun
# Tab 2: set -a; source .env; set +a && ./gradlew :control-plane:bootRun
# Tab 3: set -a; source .env; set +a && ./gradlew :execution-service:bootRun

# Full check (compiles + runs the test suite; CP/Exec ITs spin their own containers)
./gradlew check
```

Smoke tests:

```bash
# 1. Mint OPERATOR token — tenant creation is role-gated (confirmed: 201 only with OPERATOR)
OPERATOR_JWT=$(docker/jwks/mint-jwt.sh --roles OPERATOR)

# 2. Create tenant
TENANT_RESPONSE=$(curl -s -X POST "${CONTROL_PLANE_URL}/v1/tenants" \
  -H "Authorization: Bearer $OPERATOR_JWT" \
  -H "Content-Type: application/json" \
  -d '{"name":"smoke-test-tenant"}')
echo "$TENANT_RESPONSE"
TENANT_ID=$(echo "$TENANT_RESPONSE" | grep -o '"tenantId":"[^"]*"' | cut -d'"' -f4)
echo "TENANT_ID=$TENANT_ID"

# 3. Mint tenant-scoped JWT & write directly to .env
export TENANT_JWT=$(docker/jwks/mint-jwt.sh --tenant "$TENANT_ID")
sed -i '' "s|^TENANT_JWT=.*|TENANT_JWT=$TENANT_JWT|" .env 2>/dev/null || sed -i "s|^TENANT_JWT=.*|TENANT_JWT=$TENANT_JWT|" .env

# 4. Create rule-set — real step, never "{}"
RS_RESPONSE=$(curl -s -X POST "${CONTROL_PLANE_URL}/v1/rule-sets" \
  -H "Authorization: Bearer $TENANT_JWT" \
  -H "Content-Type: application/json" \
  -d '{"name":"smoke-test-ruleset","definition":"[{\"stepId\":\"step1\",\"targetUrl\":\"http://localhost:9090/-/healthy\",\"method\":\"GET\"}]"}')
echo "$RS_RESPONSE"
RULE_SET_ID=$(echo "$RS_RESPONSE" | grep -o '"ruleSetId":"[^"]*"' | cut -d'"' -f4)
RULE_SET_VERSION=$(echo "$RS_RESPONSE" | grep -o '"version":[0-9]*' | cut -d':' -f2)
echo "RULE_SET_ID=$RULE_SET_ID  VERSION=$RULE_SET_VERSION"

# 5. Create scenario
SC_RESPONSE=$(curl -s -X POST "${CONTROL_PLANE_URL}/v1/scenarios" \
  -H "Authorization: Bearer $TENANT_JWT" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"smoke-test\",\"ruleSetId\":\"$RULE_SET_ID\",\"ruleSetVersion\":$RULE_SET_VERSION}")
echo "$SC_RESPONSE"
SCENARIO_ID=$(echo "$SC_RESPONSE" | grep -o '"scenarioId":"[^"]*"' | cut -d'"' -f4)
echo "SCENARIO_ID=$SCENARIO_ID"

# 6. Read via Gateway, capture ETag
GET_HEADERS=$(curl -si -X GET "${EDGE_GATEWAY_URL}/v1/scenarios/$SCENARIO_ID" \
  -H "Authorization: Bearer $TENANT_JWT")
ETAG=$(echo "$GET_HEADERS" | grep -i '^etag:' | tr -d '\r' | sed 's/.*"\(.*\)".*/\1/')
echo "ETAG=$ETAG"

# 7. Trigger replay
curl -i -X POST "${EDGE_GATEWAY_URL}/v1/scenarios/${SCENARIO_ID}:run" \
  -H "Authorization: Bearer $TENANT_JWT" \
  -H "If-Match: \"$ETAG\"" \
  -H "Idempotency-Key: $(uuidgen)"

# 8. Confirm terminal state
docker exec chaosforge-postgres psql -U "${DB_USERNAME}" -d "${EXEC_DB_NAME}" -c \
  "SELECT status, outcome, finished_at - started_at AS duration FROM scenario_run WHERE scenario_id = '$SCENARIO_ID';"

# 9. Cross-tenant isolation
TENANT2_RESPONSE=$(curl -s -X POST "${CONTROL_PLANE_URL}/v1/tenants" \
  -H "Authorization: Bearer $OPERATOR_JWT" \
  -H "Content-Type: application/json" \
  -d '{"name":"smoke-test-tenant-2"}')
REAL_TENANT2_ID=$(echo "$TENANT2_RESPONSE" | grep -o '"tenantId":"[^"]*"' | cut -d'"' -f4)
export TENANT2_JWT=$(docker/jwks/mint-jwt.sh --tenant "$REAL_TENANT2_ID")

curl -i -X GET "${EDGE_GATEWAY_URL}/v1/scenarios/$SCENARIO_ID" \
  -H "Authorization: Bearer $TENANT2_JWT"    # expect 404
```

### AI authoring (Control Plane only)

On memory-constrained hosts (≤8GB), override before pulling:

```bash
# Override the model (Compose and Spring Boot both need to see it)
sed -i '' 's/OLLAMA_MODEL=.*/OLLAMA_MODEL=qwen2.5-coder:1.5b/' .env 2>/dev/null \
  || sed -i 's/OLLAMA_MODEL=.*/OLLAMA_MODEL=qwen2.5-coder:1.5b/' .env

docker compose -f docker-compose_chaosforge.yml --profile ai up -d
docker exec chaosforge-ollama ollama pull qwen2.5-coder:1.5b

# Control Plane reads OLLAMA_MODEL once at boot — restart it to pick up the new value:
# Tab 2: set -a; source .env; set +a && ./gradlew :control-plane:bootRun

# AI authoring — OPERATOR role required
export AI_DRAFT_JWT=$(docker/jwks/mint-jwt.sh --tenant "$TENANT_ID" --roles OPERATOR)
curl -s -X POST "${CONTROL_PLANE_URL}/v1/ai/scenario-drafts" \
  -H "Authorization: Bearer $AI_DRAFT_JWT" -H "Content-Type: application/json" \
  -d '{"description":"latency-test my checkout API"}'
```

---

## Observability

`/actuator/prometheus` on every service; Prometheus scrapes all three; Grafana is **auto-provisioned**
(datasource + a **ChaosForge — SLIs** dashboard).

```
open http://localhost:9090   # Prometheus
open http://localhost:3000   # Grafana  (admin / chaosforge, or anonymous viewer)
```

Key SLIs: outbox lag (oldest-pending age) · outbox DEAD count · CAS 409 rate · replay outcomes ·
scenario-execution outcomes · DLQ-routed by reason · inbox dedup rate · AI authoring by outcome ·
Kafka command-consumer lag · HTTP P99 per service.

**`tenant_id` is never a Prometheus label** (cardinality bomb) — tenant data lives on traces and
structured logs only.

**Alerting (C26/C27).** [`docker/prometheus/alerts.yml`](docker/prometheus/alerts.yml) ships the two
gated SLO alerts — `ScenarioCompletionRateBelowSLO` (≥95% terminal/10m) and
`StuckScenarioBudgetExceeded` (≤1% swept-INCOMPLETE/15m) — plus operational backstops (hard-poison in
DLQ, outbox DEAD, relay lag, command-accept P99), each with a response **runbook** in
[`docs/runbooks/`](docs/runbooks/). Rules are loaded and evaluating in Prometheus (verified via
`promtool check` and `/api/v1/rules`); Alertmanager *routing* is the production add-on (the lab has
none — alerts fire in Prometheus/Grafana but page no one).

---

## Tested vs. deferred (honest matrix)

`./gradlew check` runs the full suite green — every test below against real infrastructure
(Testcontainers Postgres + Redis + Kafka; mock broker only for the outbox-crash unit test; real TLS
sockets for the mTLS handshake tests; in-process JFR for the VT-pinning harness; real Avro
`SchemaCompatibility` for the FULL_TRANSITIVE gate; real container pause/stop for the executor fault
tests; a real local HTTP server for the steady-state probe).

| Scenario | Proven by |
|---|---|
| CAS mutual exclusion (real 2-thread race) | `ReplayCriticalSectionIT` |
| Stale `If-Match` → 409 | `ReplayCriticalSectionIT` |
| Stale `x-replay-version` → FENCING_VIOLATION | `ClaimPhaseIT` |
| Cross-tenant → 404, no version disclosed | `TenantIdentityProvenanceIT` (full Security filter chain) |
| Tenant identity from verified JWT, not `X-Tenant-Id` (C10) | `TenantIdentityProvenanceIT` |
| OutboxPoller crash → PENDING survives → retried → SENT | `OutboxPollerCrashIT` |
| Duplicate `message_id` → exactly one effect | `ClaimPhaseIT` |
| 100 cold-cache reads → one source load | `TwoLevelCacheIT` |
| `UPDATE`/`DELETE` on a rule-set version → rejected | `RuleSetAppendOnlyIT` |
| Poison Avro → SCHEMA_INVALID; partition advances (no stall) | `CommandDecoderTest`, `DlqRoutingE2EIT` |
| Crash-after-claim → reaper marks run `INCOMPLETE` | `IncompleteRunSweepIT` |
| DLQ routing, retry republish, `RETRY_EXHAUSTED`, hard-poison not replayed | `DlqRoutingE2EIT`, `DlqRetryPolicyTest`, `DlqRepublisherTest` |
| mTLS: `client-auth: need` rejects a no-cert caller | `MtlsControlPlaneClientHandshakeTest`, `MtlsControlPlaneWebClientHandshakeTest` |
| Zero VT-pinning: JDBC on a request VT doesn't starve a carrier | `VirtualThreadJdbcPinningIT` |
| Client idempotency (lost 202 → original token) | `ReplayCriticalSectionIT` |
| Avro schema `FULL_TRANSITIVE`; a violating change breaks the build (C29) | `SchemaFullTransitiveCompatibilityTest` |
| Inbox/outbox purge by partition-drop; dedup preserved (C28) | `PartitionMaintenanceIT`, `OutboxPartitionPurgeIT` |
| Kill switch + in-executor deadline under PG-down / broker-partition (C19) | `KillSwitchTest`, `ExecutePhaseControlsTest`, `ExecutorControlsUnderFaultIT`, `ExecOutboxRelayIT` |
| Auto-abort on steady-state breach, wired to target health (C20) | `SteadyStateCheckTest`, `HttpTargetHealthProbeTest`, `SteadyStateAbortPipelineIT` |
| ArchUnit: no `.block()` · no `Mono`/`Flux` in CP/Exec · no advisory lock · no Spring AI in Exec · no `findById` · no `findLatest` | 10 ArchUnit rules |

**Deferred** (core logic *is* covered above at the component level; only the outer wiring/harness is
outstanding): step-level `Idempotency-Key` stability.

---

## Lab Benchmark Disclosure

**This is a lab artifact. Any number it produces is a lab number. Do not read production behaviour
into it.** Specifically:

- **Topology is RF=1.** `acks=all` confirms the sole leader — it is *not* a durability claim.
- **Redis is single-instance.** Rate limiting is globally consistent via a Lua sliding window, but a
  Redis failure disables rate limiting entirely — **fail-open by design** (availability over
  rate-limit correctness during a Redis outage).
- **Ollama runs on CPU** and is slow (10–60 s for `llama3.1:8b`). AI latency is not representative of
  anything; the `BeanOutputConverter` schema gate rejects malformed output with no auto re-prompt.
- **mTLS is implemented but profile-gated off by default** (ADR-0531): dev/tests talk plain HTTP; the
  `mtls` profile turns on mutual TLS (`server.ssl.client-auth: need`) with a self-signed internal CA and
  **manual** cert rotation (no CRL/OCSP). The Gateway public listener stays HTTP. Automate rotation and
  add a public CA cert before any real deployment.
- **Free-tier footprint:** only the Edge Gateway is deployable free; CP + Execution are Compose-local.
- **Cache staleness** is bounded by the affected L2 TTL when the invalidation bus is down (≤ 1 h
  tenants, ≤ 5 m scenarios); `rule_sets` are exempt by construction (append-only).

The value here is the *argument and the proof*, not a throughput figure.

---

## Where the decisions live

- **Architecture-decision records:** For the complete, single-source-of-truth list, see the [ADR Index](docs/adrs/README.md). The replay engine is ADR-0522 (CAS)
  narrowed by **ADR-0528** (ownership-first, idempotency, outbox hardening); Avro binary is ADR-0525;
  `FULL_TRANSITIVE` compatibility is ADR-0527; tenant-identity provenance is ADR-0524.
- **Enforced constraints:** the per-service rules (no `.block()`, no `Mono`/`Flux` in CP/Exec, no
  advisory lock, no Spring AI in Exec, no `findById`) are not documentation — they're ArchUnit tests
  that fail the build. See the `*ArchTest` classes and the CI `.block()` grep.

---

## Status

Built step-by-step against the engineering spec: infra → schema → domain → gateway → outbox relay →
execution consumer → cache → resilience → AI → observability → tests. **Every engineering acceptance
gate is now closed** (C10 tenant-provenance, C14/C16 poison-pill + reaper, C19 kill switch + bounded
under PG-down/broker-partition, C20 steady-state auto-abort, C26/C27 SLO alerts + runbooks, C28
partition-drop purge, C29 FULL_TRANSITIVE schema gate). The one remaining gate,
**C31**, is not code: it is a *survived staging game day*. The full procedure is written
([`docs/runbooks/game-day-c31.md`](docs/runbooks/game-day-c31.md)); closing it requires running the
exercise against a staging stack. This remains a capability demonstration, not a production deployment.
