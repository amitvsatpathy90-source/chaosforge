package io.chaosforge.schema.tooling;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.avro.Schema;

/**
 * Deploy/CI-time schema registration against Apicurio. NOT invoked by {@code ./gradlew check} or any
 * service's {@code bootRun} — see the {@code avroSchemaRegister} Gradle task, which is deliberately
 * not a {@code check} dependency.
 *
 * <p>Registry I/O cannot happen inside CP/Exec's transactional codecs ({@code AvroCommandPayloadCodec},
 * {@code AvroResultPayloadCodec} — both explicitly local {@code SpecificDatumWriter}s for exactly this
 * reason). This class is where schema-rules.md's "producers register before send" actually happens
 * instead: once, at build/deploy time, never per message. The running services never call Apicurio.
 *
 * <p>{@code ifExists=FIND_OR_CREATE_VERSION} makes re-running against an already-registered,
 * unchanged schema a no-op rather than a 409 — safe to run on every CI build, not just
 * schema-changing ones.
 */
public final class AvroSchemaRegistrar {

    private AvroSchemaRegistrar() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: AvroSchemaRegistrar <registryBaseUrl> <avroSourceDir>");
        }
        String registryUrl = args[0];
        Path avroDir = Path.of(args[1]);

        // Step.avsc has no topic of its own -- it's ScenarioRunCommand's nested "steps" element type.
        // Parsed first so the shared Schema.Parser can resolve the bare-name reference when parsing
        // ScenarioRunCommand.avsc next, the same ordering dependency the Avro Gradle plugin's own
        // codegen relies on. Schema#toString() on the result is fully self-contained (nested types
        // inlined) -- avoids relying on Apicurio's separate artifact-references API here.
        Schema.Parser parser = new Schema.Parser();
        parser.parse(avroDir.resolve("Step.avsc").toFile());

        Map<String, String> topicSubjects = new LinkedHashMap<>();
        topicSubjects.put("chaosforge.scenario.commands.v1-value", "ScenarioRunCommand.avsc");
        topicSubjects.put("chaosforge.scenario.results.v1-value", "ScenarioRunResult.avsc");

        HttpClient client = HttpClient.newHttpClient();
        for (Map.Entry<String, String> entry : topicSubjects.entrySet()) {
            Schema schema = parser.parse(avroDir.resolve(entry.getValue()).toFile());
            register(client, registryUrl, entry.getKey(), schema.toString());
        }
    }

    private static void register(HttpClient client, String registryUrl, String artifactId, String schemaJson)
            throws Exception {
        // Body shape (artifactId / artifactType / firstVersion.content.content) is corroborated by
        // Apicurio 3.x documentation and issue-tracker references, NOT confirmed against the
        // authoritative OpenAPI spec directly -- that fetch failed. Verify against a live instance
        // before trusting this in CI: `docker compose up apicurio` (already in this repo, $0 cost)
        // and either run this task against it or check
        // http://localhost:8086/apis/registry/v3/.tmp (Swagger UI) for the real CreateArtifact schema.
        String body = "{"
                + "\"artifactId\":\"" + artifactId + "\","
                + "\"artifactType\":\"AVRO\","
                + "\"firstVersion\":{\"content\":{"
                + "\"content\":" + jsonQuote(schemaJson) + ","
                + "\"contentType\":\"application/json\""
                + "}}}";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(registryUrl + "/groups/chaosforge/artifacts?ifExists=FIND_OR_CREATE_VERSION"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2) {
            throw new IllegalStateException(
                    "Apicurio registration failed for " + artifactId + ": HTTP " + response.statusCode()
                            + " " + response.body());
        }
        System.out.println("Registered " + artifactId + " with Apicurio (" + registryUrl + ")");
    }

    private static String jsonQuote(String raw) {
        // schemaJson is already valid JSON (Schema#toString()) but needs embedding as a JSON STRING
        // value here, so escape backslash and quote only -- Schema#toString() never emits control
        // characters that would need further escaping.
        return "\"" + raw.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
