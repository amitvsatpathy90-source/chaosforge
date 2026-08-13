package io.chaosforge.schema;

import static org.assertj.core.api.Assertions.assertThat;

import io.chaosforge.schema.v1.ScenarioRunCommand;
import io.chaosforge.schema.v1.ScenarioRunResult;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.junit.jupiter.api.Test;

/**
 * Acceptance gate <b>C29</b> — the canonical Avro subjects are <b>FULL_TRANSITIVE</b> (ADR-0527). This
 * is the build-time gate referenced by {@code .architecture specifications/rules/schema-rules.md}: it runs the real Avro
 * {@link SchemaCompatibility} algorithm — the same backward/forward checks Apicurio applies — against
 * the <b>registered schema history</b> under {@code src/test/resources/avro-history/}, so a schema edit
 * that would break a rolling deploy or a rollback fails {@code ./gradlew check} instead of surfacing as
 * a runtime {@code SCHEMA_INVALID} on a live topic.
 *
 * <p>FULL_TRANSITIVE means a candidate schema is both backward- and forward-compatible with <b>every</b>
 * previously registered version, not merely the latest — so any consumer version reads any producer
 * version and rolling deploys in any order (and rollbacks) are safe. The
 * {@link #transitiveViolation_compatibleWithPredecessorButNotAncestor_isRejected()} control is what
 * proves the gate is genuinely <i>transitive</i>: a change that is FULL against its immediate
 * predecessor but not against an older ancestor is rejected — plain FULL would have waved it through.
 */
class SchemaFullTransitiveCompatibilityTest {

    private static final String COMMAND_HISTORY_DIR = "/avro-history/ScenarioRunCommand";
    private static final String RESULT_HISTORY_DIR = "/avro-history/ScenarioRunResult";

    // ---- The real gate: each shipped canonical schema vs its registered history -----------------

    @Test
    void shippedCommandSchema_isFullTransitiveWithItsRegisteredHistory() throws Exception {
        Schema current = ScenarioRunCommand.getClassSchema();
        List<Schema> history = loadRegisteredHistory(COMMAND_HISTORY_DIR);

        assertThat(history)
                .as("there must be at least one registered version to check against (avro-history/)")
                .isNotEmpty();
        assertThat(incompatibleAncestors(history, current))
                .as("shipped ScenarioRunCommand schema breaks FULL_TRANSITIVE vs a registered version")
                .isEmpty();
    }

    @Test
    void shippedResultSchema_isFullTransitiveWithItsRegisteredHistory() throws Exception {
        Schema current = ScenarioRunResult.getClassSchema();
        List<Schema> history = loadRegisteredHistory(RESULT_HISTORY_DIR);

        assertThat(history)
                .as("there must be at least one registered version to check against (avro-history/)")
                .isNotEmpty();
        assertThat(incompatibleAncestors(history, current))
                .as("shipped ScenarioRunResult schema breaks FULL_TRANSITIVE vs a registered version")
                .isEmpty();
    }

    // ---- Controls: the check correctly classifies each evolution kind ---------------------------

    @Test
    void addingOptionalFieldWithDefault_isCompatible() {
        Schema v1 = parse(BASE);
        Schema v2 = parse(BASE_PLUS_OPTIONAL);
        assertThat(incompatibleAncestors(List.of(v1), v2))
                .as("adding a field WITH a default is FULL_TRANSITIVE-safe")
                .isEmpty();
    }

    @Test
    void addingRequiredFieldWithoutDefault_breaksCompatibility() {
        Schema v1 = parse(BASE);
        Schema bad = parse(BASE_PLUS_REQUIRED);
        // A new reader of old data has no value for the required field and no default → not backward-compatible.
        assertThat(isFull(v1, bad))
                .as("adding a required field (no default) must NOT be FULL")
                .isFalse();
    }

    @Test
    void removingRequiredFieldWithoutDefault_breaksCompatibility() {
        Schema v1 = parse(BASE);
        Schema bad = parse(BASE_MINUS_REQUIRED);
        // An old reader still expects the removed required field; new data omits it → not forward-compatible.
        assertThat(isFull(v1, bad))
                .as("removing a required field must NOT be FULL")
                .isFalse();
    }

    /**
     * The load-bearing transitive proof. {@code t3} is FULL with its immediate predecessor {@code t2}
     * but NOT with the older ancestor {@code t1} (it reintroduces a field with an incompatible type).
     * A pairwise-latest (plain FULL) check passes; FULL_TRANSITIVE — checking every ancestor — rejects.
     */
    @Test
    void transitiveViolation_compatibleWithPredecessorButNotAncestor_isRejected() {
        Schema t1 = parse(T1_FIELD_INT);
        Schema t2 = parse(T2_FIELD_REMOVED);
        Schema t3 = parse(T3_FIELD_READDED_AS_STRING);

        assertThat(isFull(t2, t3))
                .as("t3 IS FULL with its immediate predecessor t2 — plain FULL would accept it")
                .isTrue();
        assertThat(incompatibleAncestors(List.of(t1, t2), t3))
                .as("FULL_TRANSITIVE must reject t3 because it breaks against the older ancestor t1")
                .containsExactly(t1);
    }

    // ---- FULL_TRANSITIVE check (the algorithm the Gradle gate enforces) -------------------------

    /** Ancestors (of the full history) that {@code candidate} is NOT FULL-compatible with. */
    private static List<Schema> incompatibleAncestors(List<Schema> history, Schema candidate) {
        List<Schema> broken = new ArrayList<>();
        for (Schema ancestor : history) {
            if (!isFull(ancestor, candidate)) {
                broken.add(ancestor);
            }
        }
        return broken;
    }

    /** FULL = backward (new reads old) AND forward (old reads new). */
    private static boolean isFull(Schema older, Schema newer) {
        boolean backward = canRead(newer, older);   // reader = new, writer = old
        boolean forward = canRead(older, newer);     // reader = old, writer = new
        return backward && forward;
    }

    private static boolean canRead(Schema reader, Schema writer) {
        return SchemaCompatibility.checkReaderWriterCompatibility(reader, writer)
                .getType() == SchemaCompatibilityType.COMPATIBLE;
    }

    private static Schema parse(String json) {
        return new Schema.Parser().parse(json);   // fresh parser per call — history files redefine names
    }

    private List<Schema> loadRegisteredHistory(String historyDir) throws URISyntaxException, IOException {
        Path dir = Paths.get(getClass().getResource(historyDir).toURI());
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(p -> p.toString().endsWith(".avsc"))
                    .sorted()
                    .map(SchemaFullTransitiveCompatibilityTest::parseFile)
                    .toList();
        }
    }

    private static Schema parseFile(Path p) {
        try {
            return new Schema.Parser().parse(p.toFile());
        } catch (IOException e) {
            throw new IllegalStateException("could not parse history schema " + p, e);
        }
    }

    // ---- Inline control schemas -----------------------------------------------------------------

    private static final String BASE = """
        {"type":"record","name":"Cmd","namespace":"io.chaosforge.schema.ctl","fields":[
          {"name":"scenarioId","type":"string"},
          {"name":"replayVersion","type":"long"}
        ]}""";

    private static final String BASE_PLUS_OPTIONAL = """
        {"type":"record","name":"Cmd","namespace":"io.chaosforge.schema.ctl","fields":[
          {"name":"scenarioId","type":"string"},
          {"name":"replayVersion","type":"long"},
          {"name":"note","type":["null","string"],"default":null}
        ]}""";

    private static final String BASE_PLUS_REQUIRED = """
        {"type":"record","name":"Cmd","namespace":"io.chaosforge.schema.ctl","fields":[
          {"name":"scenarioId","type":"string"},
          {"name":"replayVersion","type":"long"},
          {"name":"region","type":"string"}
        ]}""";

    private static final String BASE_MINUS_REQUIRED = """
        {"type":"record","name":"Cmd","namespace":"io.chaosforge.schema.ctl","fields":[
          {"name":"replayVersion","type":"long"}
        ]}""";

    // Transitive chain: t1 has an int field with a default; t2 removes it (default makes that FULL);
    // t3 re-adds it as a string (FULL vs t2, but int↔string is incompatible vs t1).
    private static final String T1_FIELD_INT = """
        {"type":"record","name":"T","namespace":"io.chaosforge.schema.ctl","fields":[
          {"name":"scenarioId","type":"string"},
          {"name":"weight","type":"int","default":0}
        ]}""";

    private static final String T2_FIELD_REMOVED = """
        {"type":"record","name":"T","namespace":"io.chaosforge.schema.ctl","fields":[
          {"name":"scenarioId","type":"string"}
        ]}""";

    private static final String T3_FIELD_READDED_AS_STRING = """
        {"type":"record","name":"T","namespace":"io.chaosforge.schema.ctl","fields":[
          {"name":"scenarioId","type":"string"},
          {"name":"weight","type":"string","default":""}
        ]}""";
}
