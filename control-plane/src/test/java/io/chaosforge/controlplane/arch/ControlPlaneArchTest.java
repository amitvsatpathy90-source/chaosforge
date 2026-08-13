package io.chaosforge.controlplane.arch;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noMethods;
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
 * Control Plane structural invariants (control-plane-rules.md). These are bytecode/source guards that
 * fail the build on regression — no infra needed.
 */
class ControlPlaneArchTest {

    private static final JavaClasses CP = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("io.chaosforge.controlplane");

    @Test
    void noReactiveTypesInControlPlane() {
        // MVC + virtual threads only — Mono/Flux mix paradigms with no benefit (ADR-0500).
        noClasses().should().dependOnClassesThat().resideInAnyPackage("reactor.core.publisher..")
                .because("Control Plane is MVC + virtual threads; no reactive types")
                .check(CP);
    }

    @Test
    void noFindByIdOnRepositories() {
        // Every tenant-scoped query must carry tenantId; findById(UUID) is a cross-tenant leak (ADR-0509).
        noMethods().that().areDeclaredInClassesThat().resideInAPackage("..repository..")
                .should().haveName("findById")
                .because("tenant-scoped repos must take tenantId — no findById(UUID) (ADR-0509)")
                .check(CP);
    }

    @Test
    void replayPathUsesCasNotAdvisoryLock() throws IOException {
        // ADR-0528/0522: the replay critical section is CAS-only; pg_try_advisory_xact_lock is forbidden.
        List<String> offenders = mainSourcesContaining("pg_try_advisory");
        assertThat(offenders)
                .as("no advisory lock anywhere in the Control Plane (CAS only — ADR-0522/0528)")
                .isEmpty();
    }

    @Test
    void documentedControlPlaneSliMetricsAreEmitted() throws IOException {
        // The SLIs table in architecture specifications is only trustworthy if every name it lists actually appears in
        // code. architecture specifications is gitignored so a test can't read it — instead we pin the CP-emitted side:
        // if one of these literals is renamed, this fails and forces the table to be updated too.
        // (Exec-emitted SLIs are guarded the same way in ExecutionServiceArchTest.)
        for (String metric : List.of(
                "replay.conflict", "replay.claimed", "replay.idempotent_hit", "replay.notfound",
                "outbox.oldest_pending_age_seconds", "outbox.pending_count",
                "outbox.pending_outside_claim_window", "outbox.lease_takeovers",
                "chaosforge.partition.default_rows", "db.schema.migrations.pending")) {
            assertThat(mainSourcesContaining('"' + metric + '"'))
                    .as("SLI metric %s named in architecture specifications must be emitted by control-plane", metric)
                    .isNotEmpty();
        }
    }

    @Test
    void noIoExceptionCatchAtJsonBoundaries() throws IOException {
        // Jackson 3 throws JacksonException extends RuntimeException — catch (IOException) silently
        // misses it (ADR-0520). Guard against reintroducing IOException multi-catch around Jackson.
        List<String> offenders = mainSourcesContaining("JacksonException | ");
        assertThat(offenders)
                .as("no JacksonException|IOException multi-catch (Jackson 3 — ADR-0520)")
                .isEmpty();
    }

    /** Returns the relative paths of main sources containing the given literal. */
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
