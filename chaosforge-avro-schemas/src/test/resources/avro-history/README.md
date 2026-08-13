# Registered schema history (FULL_TRANSITIVE gate — C29 / ADR-0527)

Each subdirectory holds the **frozen, previously-registered versions** of one Avro subject (named
after the record). `SchemaFullTransitiveCompatibilityTest` loads the *current* schema (from the
generated `…getClassSchema()`) and asserts it is **FULL_TRANSITIVE** — both backward- and
forward-compatible — against **every** file here, exactly as the Apicurio registry would when
`FULL_TRANSITIVE` is the configured compatibility level.

```
avro-history/
  ScenarioRunCommand/
    v1.avsc        # the schema as registered for version 1 (fully inlined; self-contained)
    v2.avsc        # add this when you cut v2 — BEFORE editing src/main/avro
```

## Evolving a schema (expand–contract)

1. **Freeze the current version here first.** Copy the present schema into `avro-history/<Name>/vN.avsc`
   (fully inlined). This is the "register the prior version" step — it must happen *before* you edit
   `src/main/avro`, or the gate has nothing to compare against.
2. Make your change in `src/main/avro/`. Allowed under FULL_TRANSITIVE: add a field **with a default**,
   remove a field **that has a default**, widen `int`→`long`. Forbidden: add a required field, remove a
   required field, rename (use `aliases`), narrow a type. (Full table in `.architecture specifications/rules/schema-rules.md`.)
3. Run `./gradlew :chaosforge-avro-schemas:avroSchemaCompatibilityCheck`. A violation fails the build
   with the offending ancestor named — it is never a runtime consumer surprise.

The files here are **append-only history**, not a mirror of the current schema: never delete or rewrite
an old version to make the check pass — that is the check telling you the change is unsafe.
