# ChaosForge — MCP Server Design

Decisions: ADR-0543 (MCP placement + dual-audience boundary), ADR-0510 (indistinguishable 404), ADR-0518
(AI determinism boundary), ADR-0536 (authentication is not authorization). This doc is the
operational/topology companion.

---

## 1. Why embedded in the Control Plane

The tools call the same tenant-scoped services `/v1` uses. A standalone adapter would re-implement tenant
resolution and service access for no isolation gain, so the Control Plane hosts the MCP server
(`spring-ai-starter-mcp-server-webmvc`, `protocol: STATELESS`) and the Gateway fronts it.

## 2. Topology — who does what

```
   MCP client ──► Edge Gateway (WebFlux, :8080) ────────────────► Control Plane (MVC + VT, :8081)
   Bearer aud=      POST /mcp                                        MCP server (JSON-RPC, 8 tools)
   chaosforge-mcp   · @Order(1) chain, aud=chaosforge-mcp            · @Order(1) chain, aud=chaosforge-mcp
                    · per-tenant rate limit                          · JwtTenantExtractionFilter → tenant + ROLE_* + SCOPE_*
                    · gateway-mcp CB + bulkhead + 15s timeout        · @PreAuthorize per tool
                    · raw byte forward — no JSON-RPC parsing         · run_projection read cache
                    GET /.well-known/oauth-protected-resource
```

| Service | MCP responsibility |
|---|---|
| **Edge Gateway** | Authenticate (`aud=chaosforge-mcp`), tenant + per-operate-token rate-limit gate, resilience wrapper, RFC 9728 discovery. Never parses the MCP payload. |
| **Control Plane** | The entire MCP server: protocol, tool registry, tool execution, tenant-scoped logic, per-tool authorization. |
| **Execution Service** | None. It only produces the `results.v1` events behind `get_run_status`. |

## 3. The dual-audience boundary

| | `/v1` | `/mcp` |
|---|---|---|
| Matcher | default chain | `@Order(1)`, `/mcp` + `/mcp/**` |
| Required `aud` | `chaosforge` | `chaosforge-mcp` (`chaosforge.security.mcp.jwt.audience`) |
| Tenant + roles | `JwtTenantExtractionFilter` | same filter, plus allow-listed `scope` → `SCOPE_*` |

A `/v1` token is rejected at `/mcp` and an MCP token is rejected at `/v1` — both `401` with
`The aud claim is not valid`, at the Gateway and again at CP. A tenant-less MCP token is rejected at the
Gateway (`403`) and never reaches CP.

## 4. Tool inventory

| Tool | Authorization | Effect |
|---|---|---|
| `get_scenario`, `list_scenarios` | `SCOPE_chaosforge.read` | read |
| `get_rule_set`, `list_rule_sets` | `SCOPE_chaosforge.read` | read |
| `get_run_status` | `SCOPE_chaosforge.read` | read (`run_projection`) |
| `get_dlq_triage` | `hasRole('OPERATOR') and SCOPE_chaosforge.dlq` | read-only advisory (ADR-0518) |
| `draft_scenario` | `SCOPE_chaosforge.operate` | AI authoring, **never persists** |
| `start_scenario` | `SCOPE_chaosforge.operate` | **the only state-changing tool** — initiates a replay |

Every `@McpTool` must carry its own `@PreAuthorize` (ArchUnit-enforced).

## 5. What MCP can never do

No create-tenant / rule-set / scenario tool. No `mark_dlq_reviewed` (the triage watermark write is
HTTP-only and human-only). No `prepare_scenario_run` (the orchestrator has no prepare/start split, and a
per-call `idempotencyKey` would defeat ADR-0528). Creating a scenario to run stays on `/v1`.

## 6. `run_projection` — CP's first Kafka consumer

| Service | Produces | Consumes |
|---|---|---|
| Control Plane | `chaosforge.scenario.commands.v1` | `chaosforge.scenario.results.v1` (**new**) |
| Execution Service | `chaosforge.scenario.results.v1` | `chaosforge.scenario.commands.v1` |

Group `control-plane-run-projection`, `auto.offset.reset=earliest` (a first boot backfills from the
earliest retained result), `AckMode.MANUAL`. `run_projection` (`V12`) is a rebuildable cache keyed on
`(scenario_id, replay_version)`; the upsert is idempotent. Never DLQ-routed.

| Failure | Behaviour | Proven by |
|---|---|---|
| Undecodable bytes | logged, `decode_failures++`, acked, **never retried** | `RunProjectionListenerIT` (poison record does not stall the partition) |
| Transient persist failure | listener rethrows; `DefaultErrorHandler` retries in place — 1 attempt + 2 retries, 2s apart; `persist_failures++` per attempt | `RunProjectionListenerIT` — row lands after recovery, event published once |
| Retries exhausted | `persist_failures_exhausted++`, offset committed past the record, partition not wedged | `RunProjectionListenerIT` — next record still lands |

Both permanent-loss paths page `RunProjectionFailureRateHigh` (`ticket`). A dropped record leaves
`get_run_status` at `IN_PROGRESS` for that scenario; Exec's `scenario_run` holds the truth.

## 7. Configuration

| Key | Value |
|---|---|
| `spring.ai.mcp.server.*` (protocol `STATELESS`, name `chaosforge-control-plane`, version `1.0.0`) | must sit under `spring.ai`; a misplaced block silently registers no router |
| `chaosforge.security.mcp.jwt.audience` / `MCP_JWT_AUDIENCE` | `chaosforge-mcp` |
| `spring.kafka.consumer.group-id` | `control-plane-run-projection` |
| `chaosforge.kafka.results-topic` | `chaosforge.scenario.results.v1` |
| Resilience4j `gateway-mcp` | dedicated CB + bulkhead, 15s timeout |

## 8. Verification

Three tiers — do not read one as another:

1. **Process started** — each `bootRun` logs `Started …Application`.
2. **Component healthy** — `GET /actuator/health/readiness` is `UP` on `:8080`, `:8081`, `:8082`. This does
   *not* prove the Kafka consumer is connected; Spring Kafka retries broker connections silently.
3. **MCP end-to-end** — the smoke test in the README (`Running it` → MCP): `tools/list`, `start_scenario`,
   `get_run_status`, PRM discovery, and both wrong-audience rejections.

`get_run_status` is a single point-in-time check. `status` is `IN_PROGRESS` (no `run_projection` row yet) or
`TERMINAL` with an `outcome` (`COMPLETED` / `ABORTED` / `FAILED` / `INCOMPLETE`); do not assert a specific
outcome. Every MCP request needs `Accept: application/json, text/event-stream` even in stateless mode.
`expectedVersion` for `start_scenario` is the scenario's *current* replay version — the value in the
latest `ETag`/`get_scenario`, refreshed after any prior replay.

## 9. Limitations (lab) — carry into Known Limitations

- RFC 8707 resource-indicator-at-issuance is N/A: the JWKS stub has no token endpoint; tokens are minted by
  `docker/jwks/mint-jwt.sh`.
- `scopes_supported` advertises only `chaosforge.read`; `operate` and `dlq` are deliberately undisclosed.
- A record `run_projection` gives up on stays stale until that scenario is replayed again.
- The promtool cases for `RunProjectionFailureRateHigh` are a manual gate, like the other alert tests.
- The operate-token rate limit falls back to a local per-pod window during a Redis outage (upper bound, N pods = N × limit). Read-scope tokens are not covered by the token limiter, so agents should poll `get_run_status` with a read-only token. See ADR-0543 Amendment 2.
