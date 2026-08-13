# Runbook: DLQ & outbox backstops

Covers the operational (`ticket`) alerts: `HardPoisonInDLQ`, `DlqTriageBacklog`,
`OutboxDeadLetterPresent`, `OutboxRelayLagging`, `CommandAcceptLatencyP99High`. None is yet an SLO
breach, but each becomes one if left alone.

---

## `HardPoisonInDLQ`

**Fires:** a record with `x-dlq-reason ∈ {SCHEMA_INVALID, STEP_FAILED, RETRY_EXHAUSTED}` reached the
DLQ in the last 10m. These are **non-replayable** — the retry consumer never touches them; they wait
for a human. (`FENCING_VIOLATION` is deliberately excluded: it is expected under concurrent replay and
safe to discard — a higher `replay_version` already superseded it.)

**Triage:**
```bash
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic chaosforge.scenario.commands.v1.DLQ --from-beginning --property print.headers=true
```

| `x-dlq-reason` | Meaning | Action |
|---|---|---|
| `SCHEMA_INVALID` | payload could not be decoded / tenant re-verify failed at step 1 | producer/schema bug — fix the producer or the registered schema; the record is not recoverable as-is |
| `STEP_FAILED` | a step failed *semantically* (scenario logic) | fix the scenario/rule-set; not replayable |
| `RETRY_EXHAUSTED` | a replayable record hit `chaosforge.dlq.retry.max-attempts` | the target was down past the budget — confirm it is back, then re-issue the replay from the Control Plane |

The AI DLQ-triage endpoint (`GET /v1/dlq/{topic}/triage/{offset}?partition=N`, CP, OPERATOR-gated) can
*suggest* `DISCARD | INVESTIGATE | REPLAY_WHEN_READY` — advisory only; it never acks or republishes.
The human decides.

**Do not** hand-republish a hard-poison record to the main topic. If a `RETRY_EXHAUSTED` scenario must
run again, go through `POST /scenarios/{id}:run` (fresh `Idempotency-Key`) so a clean command is minted.

**After triaging, record it** so `DlqTriageBacklog` clears — advance the review watermark past the
records you dispositioned (below).

---

## `DlqTriageBacklog`

**Fires:** `chaosforge_dlq_untriaged_depth{job="control-plane"} > 0` for **1h** on some `(topic,
partition)`. This is the standing **level** signal (ADR-0542) that `HardPoisonInDLQ` — a *rate* — cannot
give: it answers "how many records are sitting in the DLQ awaiting human triage **right now**".

Depth is `endOffset − reviewedOffset`, where `reviewedOffset` is the exclusive high-water an operator has
recorded as reviewed. It is **not** derived from Kafka consumer-group lag (which reaches 0 with poison
still present, because the retry consumer acks hard poison it never republishes — the whole reason this
gauge exists, arch-audit F5).

**Two things to understand before acting:**
- The gauge counts **everything** past the watermark, including replayable records the retry lane already
  drained but that Kafka still retains. It is a *review* backlog, not a *hard-poison* backlog — pair it
  with `HardPoisonInDLQ` (which tells you whether any of the backlog is non-replayable).
- The gauge **trusts** the watermark: it reports what an operator *claims* to have reviewed
  (`reviewed_by`/`reviewed_at` are audited), not verified disposition. Advancing it is the triage action,
  and it is OPERATOR-gated for exactly that reason.

**Triage:** inspect the un-reviewed records (per `HardPoisonInDLQ` above), then advance the watermark to
the end offset you have reviewed through:

```bash
# current end offset for the partition:
kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 \
  --topic chaosforge.scenario.commands.v1.DLQ --partitions 0 --time -1

# record the review (OPERATOR JWT); reviewedOffset is exclusive (all offsets < it are dispositioned):
curl -X PUT "http://localhost:8081/v1/dlq/chaosforge.scenario.commands.v1.DLQ/reviewed?partition=0" \
  -H "Authorization: Bearer $OPERATOR_JWT" -H 'Content-Type: application/json' \
  -d '{"reviewedOffset": <end-offset>}'
```

The advance is monotonic (a lower offset is a no-op) and writes only the CP watermark row — never a Kafka
offset, never a replay or ack. Do **not** advance the watermark to silence the alert without actually
reviewing: that is the one abuse the audit columns exist to catch.

> **Retention caveat.** DLQ topic retention must exceed your triage SLA. A record deleted by retention
> while still un-reviewed is gone (and the gauge stops counting it) — the backlog must be worked before
> `retention.ms` elapses, exactly as with the outbox partition-drop fuse below.

---

## `OutboxDeadLetterPresent`

**Fires:** `outbox_dead_count{job="control-plane"} > 0` — a row could not be published after the full
retry budget and was parked `DEAD`.

```bash
psql "$CP_DB_URL"  -c "SELECT status, count(*) FROM outbox GROUP BY status;"
# Inspect the DEAD rows (no payload dump — headers/age only):
psql "$CP_DB_URL"  -c "SELECT message_id, attempts, next_attempt_at, created_at
                       FROM outbox WHERE status='DEAD' ORDER BY created_at LIMIT 20;"
```

**Cause:** the broker was unreachable for longer than `MAX_ATTEMPTS × backoff`, or the message is
structurally unpublishable. Confirm Kafka/Redpanda is healthy (`docker compose ps`,
`kafka-console-consumer` on the main topic). Once the broker is back, a `DEAD` row does **not** retry
automatically (terminal by design) — requeue it deliberately after confirming the effect has not
already been published (check the consumer inbox for its `message_id`).

The same alert and procedure apply to the **execution-service** outbox (`$EXEC_DB_URL`), which relays
result events.

---

## `OutboxRelayLagging`

**Fires:** `outbox_oldest_pending_age_seconds{job="control-plane"} > 60` for 5m. The poller normally
drains `PENDING` within seconds (`SELECT … FOR UPDATE SKIP LOCKED`, lease-on-claim).

**Check:** is the poller running (CP logs), is Kafka accepting publishes, is `outbox_pending_count`
climbing? A stuck relay with a healthy broker usually means the poller thread died or the
`@Scheduled` task is wedged — restart the Control Plane. A stuck relay with a **down** broker is
expected; this alert is then just confirming the outage — focus on Kafka.

Crash safety: a row stays `PENDING` until the `KafkaTemplate` send callback confirms broker ack, so a
crash between DB commit and ack leaves it `PENDING` and the poller retries — no lost or duplicated
publish. Lag is a throughput/availability signal, never a correctness one.

> **Why this alert is the safety net for partition-drop purge (C28).** The outbox is purged by dropping
> aged day-partitions (`PartitionMaintenance`), which is age-based, not status-aware — a partition is
> dropped once its whole day is older than retention. A healthy row is SENT within seconds, so this is
> safe; the only row that could be dropped while still un-delivered is one stuck `PENDING`/`DEAD` for
> **days**, and that row trips `OutboxRelayLagging` / `OutboxDeadLetterPresent` long before its
> partition ages out. Treat those two alerts as the guardrail: clear the stuck row before retention
> elapses and nothing undelivered is ever dropped.

---

## `CommandAcceptLatencyP99High`

**Fires:** CP HTTP P99 > 300ms for 10m (the command-accept SLO: POST → outbox commit).

**Likely causes & checks:**
- **Row-lock contention on the replay CAS** — expected to surface as fast **409**s
  (`lock_timeout=750ms`), not slow 200s. A rising P99 with rising `replay_conflict_total` means hot
  scenarios; clients should honour the jittered `Retry-After`.
- **JDBC pool starvation** — check Hikari active/pending. (VT-pinning is *not* a cause here — proven
  zero by `VirtualThreadJdbcPinningIT`.)
- **Postgres slow** — check DB CPU / `pg_stat_activity` for long statements; the CAS tx has
  `statement_timeout=5s` so a genuinely hung statement fails rather than hangs forever.

Latency that is *all* contention 409s is working as designed; latency on the success path is the real
problem — separate them with the **Replay outcomes (CP)** panel before digging.

---

## Related
- [scenario-completion-rate.md](scenario-completion-rate.md), [stuck-scenario.md](stuck-scenario.md)
  — the gated SLO alerts.
- DLQ taxonomy and replayability: [`DLQ policy specifications`](../../DLQ policy specifications).
