package io.chaosforge.gateway.arch;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.assertj.core.api.Assertions.assertThat;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Edge Gateway structural invariants (gateway-rules.md). The Gateway is the only WebFlux service —
 * blocking calls and {@code ThreadLocal} (lost across reactive thread hops) are forbidden.
 */
class GatewayRulesTest {

    private static final JavaClasses GATEWAY = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("io.chaosforge.gateway");

    @Test
    void noBlockingCalls() throws IOException {
        // GAP-02: match .block() AND the variants gateway-rules.md explicitly forbids —
        // .blockFirst() / .blockLast() / .blockOptional(). A bare ".block(" substring missed all three,
        // so a regression to any of them (real event-loop-blocking calls) passed CI silently.
        assertThat(mainSourcesMatching("\\.block(First|Last|Optional)?\\("))
                .as("no blocking Reactor call (.block/.blockFirst/.blockLast/.blockOptional) — reactive end-to-end")
                .isEmpty();
    }

    @Test
    void noRestTemplate() {
        // All outbound HTTP uses WebClient (gateway-rules.md).
        noClasses().should().dependOnClassesThat()
                .haveFullyQualifiedName("org.springframework.web.client.RestTemplate")
                .because("the gateway uses WebClient, never RestTemplate")
                .check(GATEWAY);
    }

    @Test
    void noThreadLocalTenantContext() throws IOException {
        // tenant_id propagates via Reactor Context; ThreadLocal is invisible across thread hops.
        assertThat(mainSourcesContaining("ThreadLocal"))
                .as("gateway uses Reactor Context, never ThreadLocal")
                .isEmpty();
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

    private static List<String> mainSourcesMatching(String regex) throws IOException {
        Pattern pattern = Pattern.compile(regex);
        Path root = Path.of("src/main/java");
        try (Stream<Path> paths = Files.walk(root)) {
            return paths.filter(p -> p.toString().endsWith(".java"))
                    .filter(p -> pattern.matcher(stripComments(readSilently(p))).find())   // ignore Javadoc/comments
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
