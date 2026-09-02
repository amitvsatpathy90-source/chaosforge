# ADR Index

Single source of truth for ChaosForge's Architectural Decision Records. Root `README.md` and `architecture specification` links
here instead of duplicating this table.

| ID | Decision | File |
|---|---|---|
| ADR-0500 | Edge Gateway = WebFlux/Netty; CP + Exec = MVC + virtual threads; reactive opt-in criteria applied per-service, not everywhere | [`ADR-0500.md`](ADR-0500.md) |
| ADR-0501 | Three-service split (Gateway / Control Plane / Execution Service) along correctness-safe seams; each boundary demonstrates a distinct load-bearing claim | [`ADR-0501.md`](ADR-0501.md) |
| ADR-0502 | Replay mutex v1: Postgres advisory lock + monotonic `replay_version` + fencing tokens; Redlock rejected per Kleppmann — **SUPERSEDED by ADR-0522**, narrowed by ADR-0528 | [`ADR-0502.md`](ADR-0502.md) |
| ADR-0503 | Rule sets append-only by `(rule_set_id, version)`; no `UPDATE`; replay always loads the pinned tuple | [`ADR-0503.md`](ADR-0503.md) |
| ADR-0504 | Edge Gateway runs Caffeine L1-only cache; no Redis L2 — avoids reactive Kafka-invalidation complexity for rarely-mutated entities | [`ADR-0504.md`](ADR-0504.md) |
| ADR-0505 | Tracing disabled on the free-tier deployed gateway — amended: local Tempo stack was never built; Micrometer/Brave MDC correlation only, no span export anywhere | [`ADR-0505.md`](ADR-0505.md) |
| ADR-0506 | `x-replay-*` namespaced outbox header extension — the single documented deviation from the P1 outbox canonical | [`ADR-0506.md`](ADR-0506.md) |
| ADR-0507 | OTel stretch experiment confined to the Execution Service only; P1 baseline (Micrometer + Brave) unchanged elsewhere | [`ADR-0507.md`](ADR-0507.md) |
| ADR-0508 | DLQ failure-class taxonomy, 5 classes — amended: header renamed `x-failure-class` → `x-dlq-reason` | [`ADR-0508.md`](ADR-0508.md) |
| ADR-0509 | Three-layer tenant isolation: gateway JWT filter, service `TenantContext` ThreadLocal, repository mandatory-predicate (no `findById`) | [`ADR-0509.md`](ADR-0509.md) |
| ADR-0510 | Cross-tenant access returns 404, never 403 — no resource-existence leak | [`ADR-0510.md`](ADR-0510.md) |
| ADR-0511 | Free-tier-only deploy, $0/month cost ceiling; only the Edge Gateway is deployed publicly | [`ADR-0511.md`](ADR-0511.md) |
| ADR-0512 | Lab benchmarks must disclose the full rig (10 required fields, e.g. CPU/RAM/JVM/Kafka config/warmup/window) or the benchmark is removed and re-run | [`ADR-0512.md`](ADR-0512.md) |
| ADR-0513 | OTel-on-Execution-Service experiment outcome — pre-structured decision frame for the Week 11 game day | [`ADR-0513.md`](ADR-0513.md) |
| ADR-0514 | OTel Collector topology + tail-sampling config, triggered only if ADR-0513 = ADOPT; Option B (Collector) chosen as default | [`ADR-0514.md`](ADR-0514.md) |
| ADR-0515 | Cross-cutting chaos-experiment outcomes — pre-structured decision frame for the Week 12 game day (6 hypotheses) | [`ADR-0515.md`](ADR-0516.md) |
| ADR-0516 | RPE-specific deviations from ChaosForge canonical patterns — 6 justified divergences (Redis-as-source-of-truth, UUIDv5 alert dedup, reactive Lettuce in the hot path, Kafka EOS via `transactional.id`, `synchronous_commit=off`, agentic vs one-shot AI) plus build tool and no-replay-engine; a cross-project comparison ADR, numbered in CF but scoped to RPE | [`ADR-0516.md`](ADR-0516.md) |
| ADR-0517 | Spring AI 1.1.x on Boot 3.x — **SUPERSEDED same-day** by ADR-0521 following the Boot 4 baseline shift | [`ADR-0517.md`](ADR-0517.md) |
| ADR-0518 | AI determinism boundary: LLM called only at authoring time and advisory DLQ triage, never on the replay path; structural isolation — amended by ADR-0542 (non-AI operator write verb) | [`ADR-0518.md`](ADR-0518.md) |
| ADR-0519 | PII egress controls for LLM prompts: local Ollama default, field redaction, structural injection defence | [`ADR-0519.md`](ADR-0519.md) |
| ADR-0520 | Spring Boot 4.1.x platform baseline (EOL-driven migration); Jackson 3 / Security 7 / Kafka 4 breaking-surface audit | [`ADR-0520.md`](ADR-0520.md) |
| ADR-0521 | Spring AI 2.0.x on Boot 4.1.x, supersedes ADR-0517; Ollama local-model default carried forward | [`ADR-0521.md`](ADR-0521.md) |
| ADR-0522 | CAS optimistic lock supersedes the Postgres advisory lock for the replay mutex — supersedes ADR-0502, narrowed by ADR-0528 | [`ADR-0522.md`](ADR-0522.md) |
| ADR-0523 | Execution transaction decomposition: short claim-tx + stateless step execution + short result-tx — fixes JDBC-pool exhaustion and rebalance-triggered re-execution | [`ADR-0523.md`](ADR-0523.md) |
| ADR-0524 | Tenant identity provenance: JWT re-verified at CP/Exec; `X-Tenant-Id` header never trusted (mTLS authenticates the channel, not the claim) | [`ADR-0524.md`](ADR-0524.md) |
| ADR-0525 | Kafka wire format: Avro binary confirmed; JSON Schema serialization rejected — amended: `FULL_TRANSITIVE` per ADR-0527; runtime uses a local Avro codec, not per-message registry calls | [`ADR-0525.md`](ADR-0525.md) |
| ADR-0526 | Kafka partition key = `scenario_id`; tenant-key rejected for head-of-line blocking | [`ADR-0526.md`](ADR-0526.md) |
| ADR-0527 | Schema compatibility mode = `FULL_TRANSITIVE`; `BACKWARD` alone is insufficient for safe rolling deploys/rollbacks | [`ADR-0527.md`](ADR-0527.md) |
| ADR-0528 | Replay critical section finalized — ownership probe strictly precedes the CAS bump (collapses cross-tenant/absent into one 404, closes the version-enumeration side-channel), plus idempotency-key claim and a hardened outbox poller (`SKIP LOCKED`, attempt ceiling, lag gauge); supersedes ADR-0502, narrows ADR-0522 — the system's headline correctness decision | [`ADR-0528.md`](ADR-0528.md) |
| ADR-0529 | DLQ retry consumer republishes with a **fresh** `message_id` over reusing the original — corrects `dlq-rules.md`'s literal wording, which would have made retries silent no-ops | [`ADR-0529.md`](ADR-0529.md) |
| ADR-0530 | Schema migrations run at startup (Boot 4 modular Flyway autoconfig fix) + schema-state observability gauges | [`ADR-0530.md`](ADR-0530.md) |
| ADR-0531 | Intra-service mTLS via Spring SSL Bundles, profile-gated, env-sourced secrets, no silent downgrade | [`ADR-0531.md`](ADR-0531.md) |
| ADR-0532 | Tenant trust on the CP `/internal` path is peer-asserted, not JWT-bound; cert-subject CN restriction implemented to narrow the trusted-peer set | [`ADR-0532.md`](ADR-0532.md) |
| ADR-0533 | Steady-state auto-abort (C20) cadence made time-based, not per-step — fixes an inert abort gate for 1–2-step scenarios | [`ADR-0533.md`](ADR-0533.md) |
| ADR-0534 | Execution blast-radius containment: one shared SSRF guard (`TargetUrlGuard`) enforced at both authoring and execution | [`ADR-0534.md`](ADR-0534.md) |
| ADR-0535 | Exactly one terminal event per run under DLQ retry: heartbeat sweep + status-guarded finalize close a double-terminal-event race | [`ADR-0535.md`](ADR-0535.md) |
| ADR-0536 | Authentication is not authorization on internal & operator surfaces: gateway policy feed moved to a peer-scoped endpoint; kill switch requires an OPERATOR role | [`ADR-0536.md`](ADR-0536.md) |
| ADR-0537 | Every silent-failure mode of a resilience mechanism must be observable — partition-trap tripwire, straggler-backlog alert, rate-limit fail-open counters | [`ADR-0537.md`](ADR-0537.md) |
| ADR-0538 | Steady-state health probes run on an isolated short-timeout client inside a top-guarded loop — prevents a self-inflicted consumer rebalance | [`ADR-0538.md`](ADR-0538.md) |
| ADR-0539 | Reject out-of-partition-window message mint-times at consumer step 1 (`CommandDecoder.verifyMintClock`) — closes the un-droppable default-partition trap at the source | [`ADR-0539.md`](ADR-0539.md) |
| ADR-0540 | Gateway rate limiting: Redis Lua sliding window, globally consistent across pods, fail-open — backfill record correcting ADR-0500's stale token-bucket wording | [`ADR-0540.md`](ADR-0540.md) |
| ADR-0541 | Deployment security posture: startup asserts the individual controls (mTLS, peer-CN, SSRF guard), not just the profile flag; unconditional hardened/unhardened gauge | [`ADR-0541.md`](ADR-0541.md) |
| ADR-0542 | Standing DLQ-depth signal: a per-`(topic, partition)` human-triage watermark in CP Postgres (`endOffset − reviewedOffset`) plus an OPERATOR-gated write verb kept off the read-only triage classes — closes arch-audit F5; explicitly re-scopes ADR-0518's "never writes state" invariant to the AI advisory path only | [`ADR-0542.md`](ADR-0542.md) |

---

## Changelog

| Date | Change |
|---|---|
| 2026-09-03 | Updated `File` column entries across the index from static file names to explicit Markdown links (`[ADR-XXX.md](ADR-XXX.md)`) for GitHub views. |
| 2026-08-09 | Index compiled from the full ADR corpus (ADR-0500–0542) |
