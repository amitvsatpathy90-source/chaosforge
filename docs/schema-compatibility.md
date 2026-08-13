# Schema compatibility & deploy ordering (C29 / ADR-0527)

Two guarantees keep the `chaosforge.scenario.commands.v1` Avro contract safe across deploys:

1. **`FULL_TRANSITIVE` compatibility**, enforced at build time — a schema change that could break a
   rolling deploy or a rollback fails `./gradlew check`, never a live consumer.
2. **Consumer-before-producer deploy ordering** — the consumer that understands a new schema version
   is rolled out before the producer that emits it.

---

## 1. The build-time gate

`./gradlew :chaosforge-avro-schemas:avroSchemaCompatibilityCheck` (a dependency of `check`, so it runs
in every build and in CI before any publish) executes
[`SchemaFullTransitiveCompatibilityTest`](../chaosforge-avro-schemas/src/test/java/io/chaosforge/schema/SchemaFullTransitiveCompatibilityTest.java).
It runs the real Avro `SchemaCompatibility` algorithm — the same backward/forward checks Apicurio
applies under the `FULL_TRANSITIVE` level — comparing the **current** generated schema against **every**
registered version in
[`avro-history/`](../chaosforge-avro-schemas/src/test/resources/avro-history/).

A violation **breaks the build**, naming the ancestor it broke against. Proven both ways: the suite
passes on the shipped schema and on a valid evolution, and **fails** when a required field (no default)
is added to the canonical schema. The
`transitiveViolation_compatibleWithPredecessorButNotAncestor_isRejected` control proves the check is
genuinely *transitive* — it rejects a change that is FULL against its immediate predecessor but not
against an older ancestor, which a plain-FULL (pairwise-latest) check would wave through.

Evolution rules and the expand–contract workflow live in
[`.architecture specifications/rules/schema-rules.md`](../.architecture specifications/rules/schema-rules.md) and
[`avro-history/README.md`](../chaosforge-avro-schemas/src/test/resources/avro-history/README.md).

---

## 2. Consumer-before-producer deploy ordering

`FULL_TRANSITIVE` makes *any* consumer version able to read *any* producer version, so ordering can
never cause a hard decode failure. Ordering still matters for **semantics**: a consumer should be ready
to *act on* a new field before a producer starts *relying on* it. The rule is therefore:

> **Roll out the consumer (Execution Service) that understands schema vN before the producer
> (Control Plane) that emits vN.**

### Pipeline stages (per schema version bump)

| Step | Action | Why it is safe |
|---|---|---|
| 0 | `avroSchemaCompatibilityCheck` is green on vN (CI gate) | vN is FULL_TRANSITIVE with all prior versions |
| 1 | **Deploy Execution Service (consumer) vN** | Still reads vN-1 traffic (forward-compat); now *also* understands vN |
| 2 | Soak — confirm consumer healthy, command-consumer lag flat | No producer emits vN yet, so this is a pure no-op upgrade |
| 3 | **Deploy Control Plane (producer) vN** | Every running consumer already understands vN |
| 4 | Verify completion-rate SLO holds (see `docs/runbooks/`) | End-to-end vN path exercised under real traffic |

### Rollback
Because vN is FULL_TRANSITIVE, rollback is the same list in reverse and equally safe: roll the producer
back to vN-1 first (consumers on vN still read vN-1 — backward-compat), then the consumer if needed.
No coordinated lock-step, no maintenance window, no `outbox`-locking DDL.

### What this guards against
Deploying the **producer first** would emit vN records that a not-yet-upgraded consumer can still
*decode* (FULL_TRANSITIVE) but might *ignore* a field that vN made meaningful — a silent semantic gap,
not a crash. Consumer-first closes that gap. The compatibility gate (part 1) guarantees the decode is
always safe regardless; the ordering guarantees the *behaviour* is too.

---

## Lab note

There is no live Apicurio registry in the test path — the gate checks against the in-repo
`avro-history/` (the registry's job, done locally and deterministically). In a real pipeline the same
`FULL_TRANSITIVE` level is also set on the Apicurio subject, and the CI gate is the pre-flight that
makes a registry rejection impossible at deploy time. Apicurio must be HA before the Control Plane
scales beyond one replica (see architecture specifications Known Limitations).
