// Avro codegen module (ADR-0525). Canonical .avsc files live in src/main/avro/.
// The Gradle Avro plugin generates type-safe Java at build time — no runtime reflection.
plugins {
    `java-library`
    alias(libs.plugins.avro)
}

dependencies {
    api(libs.avro)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj.core)
    testRuntimeOnly(libs.junit.platform.launcher)
}

avro {
    fieldVisibility.set("PRIVATE")
    outputCharacterEncoding.set("UTF-8")
    stringType.set("String")          // generate java.lang.String, not CharSequence
}

// FULL_TRANSITIVE schema-compatibility gate (ADR-0527, acceptance gate C29). The check itself is a
// JUnit test (SchemaFullTransitiveCompatibilityTest) that runs the real Avro SchemaCompatibility
// algorithm against the registered schema history under src/test/resources/avro-history/. This named
// task is the explicit "before publish" entry point CI invokes prior to registering a new schema
// version; `check` depends on it, so a FULL_TRANSITIVE violation breaks the build. It is excluded from
// the default `test` task so it executes exactly once per build.
val avroSchemaCompatibilityCheck by tasks.registering(Test::class) {
    description = "Verifies src/main/avro schemas are FULL_TRANSITIVE with their registered history (C29)."
    group = "verification"
    val testSourceSet = sourceSets["test"]
    testClassesDirs = testSourceSet.output.classesDirs
    classpath = testSourceSet.runtimeClasspath
    filter { includeTestsMatching("io.chaosforge.schema.SchemaFullTransitiveCompatibilityTest") }
}

tasks.test {
    // Runs via avroSchemaCompatibilityCheck (a `check` dependency) — exclude here to avoid a double run.
    filter {
        excludeTestsMatching("io.chaosforge.schema.SchemaFullTransitiveCompatibilityTest")
        isFailOnNoMatchingTests = false
    }
}

tasks.named("check") {
    dependsOn(avroSchemaCompatibilityCheck)
}

// Deploy/CI-time schema registration against Apicurio (AvroSchemaRegistrar.java). Deliberately NOT a
// `check` dependency -- registration is a deploy action, not a verification action, and shouldn't run
// on every local `./gradlew check`. Gated behind the compatibility check regardless: never register a
// schema that failed FULL_TRANSITIVE. Test locally against the docker-compose apicurio service before
// wiring this into any CI pipeline:
//   ./gradlew :chaosforge-avro-schemas:avroSchemaRegister -PapicurioRegistryUrl=http://localhost:8086/apis/registry/v3
val avroSchemaRegister by tasks.registering(JavaExec::class) {
    description = "Registers canonical Avro schemas with Apicurio (ifExists=FIND_OR_CREATE_VERSION). Deploy/CI-time only."
    group = "publishing"
    dependsOn(avroSchemaCompatibilityCheck)
    mainClass.set("io.chaosforge.schema.tooling.AvroSchemaRegistrar")
    classpath = sourceSets["main"].runtimeClasspath
    val registryUrl = (findProperty("apicurioRegistryUrl") as String?) ?: System.getenv("APICURIO_REGISTRY_URL")
    args = listOfNotNull(registryUrl, "src/main/avro")
    doFirst {
        requireNotNull(registryUrl) { "Set -PapicurioRegistryUrl=... or APICURIO_REGISTRY_URL env var" }
    }
}
