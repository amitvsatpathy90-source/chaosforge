package io.chaosforge.execution.arch;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.assertj.core.api.Assertions.assertThat;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Execution Service structural invariants (execution-service-rules.md). The determinism boundary
 * (ADR-0518) is enforced here: no Spring AI on the replay/Kafka path.
 */
class ExecutionServiceArchTest {

    private static final JavaClasses EXEC = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("io.chaosforge.execution");

    @Test
    void noSpringAiInExecutionService() {
        // Determinism boundary: the LLM is never on the replay path (ADR-0518).
        noClasses().should().dependOnClassesThat().resideInAnyPackage("org.springframework.ai..")
                .because("the LLM is never on the deterministic replay path (ADR-0518)")
                .check(EXEC);
    }

    @Test
    void noReactiveTypesInExecutionService() {
        // MVC + virtual threads + @KafkaListener returning void — no Mono/Flux.
        noClasses().should().dependOnClassesThat().resideInAnyPackage("reactor.core.publisher..")
                .because("Execution Service is MVC + virtual threads; @KafkaListener returns void")
                .check(EXEC);
    }

    @Test
    void rulesetLoadedByPinnedVersionNeverLatest() throws IOException {
        // Replay determinism: load by pinned (id, version); a findLatest lookup breaks it (ADR-0503).
        List<String> offenders = mainSourcesContaining("findLatest");
        assertThat(offenders)
                .as("rule sets are loaded by pinned (id, version), never latest (ADR-0503)")
                .isEmpty();
    }

    @Test
    void documentedExecutionSliMetricsAreEmitted() throws IOException {
        // Pins the exec-emitted side of architecture specifications's SLIs table — see the CP counterpart in
        // ControlPlaneArchTest for the rationale. A rename here fails the build and forces the doc.
        for (String metric : List.of(
                "inbox.duplicates_suppressed_total", "chaosforge.steady_state.breach_total",
                "chaosforge.dlq.routed", "chaosforge.executor.kill_switch.engaged",
                "outbox.oldest_pending_age_seconds", "outbox.pending_count",
                "chaosforge.partition.default_rows", "db.schema.migrations.pending")) {
            assertThat(mainSourcesContaining('"' + metric + '"'))
                    .as("SLI metric %s named in architecture specifications must be emitted by execution-service", metric)
                    .isNotEmpty();
        }
    }

    private static List<String> mainSourcesContaining(String needle) throws IOException {
        Path root = Path.of("src/main/java");
        try (Stream<Path> paths = Files.walk(root)) {
            return paths.filter(p -> p.toString().endsWith(".java"))
                    .filter(p -> stripComments(readSilently(p)).contains(needle))   // ignore Javadoc/comments
                    .map(p -> root.relativize(p).toString())
                    .toList();
        }
    }

    private static String readSilently(Path p) {
        try {
            return Files.readString(p);
        } catch (IOException e) {
            return "";
        }
    }

    /** Strips block/Javadoc comments and line comments so a rule named in docs isn't a false positive. */
    static String stripComments(String src) {
        String noBlock = src.replaceAll("(?s)/\\*.*?\\*/", " ");
        StringBuilder out = new StringBuilder();
        for (String line : noBlock.split("\n", -1)) {
            int i = line.indexOf("//");
            while (i >= 0) {
                if (i > 0 && line.charAt(i - 1) == ':') {   // keep "://" (URLs), strip real // comments
                    i = line.indexOf("//", i + 2);
                    continue;
                }
                line = line.substring(0, i);
                break;
            }
            out.append(line).append('\n');
        }
        return out.toString();
    }
}
