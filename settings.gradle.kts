pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.PREFER_PROJECT)
    repositories {
        mavenCentral()
        // Spring AI 2.0.x is GA on Maven Central. Uncomment only if a pre-GA pin is ever required:
        // maven { url = uri("https://repo.spring.io/milestone") }
    }
}

rootProject.name = "chaosforge"

// Three deliberate runtime surfaces (ADR-0500) + two shared libraries. Do not collapse.
include(
    "chaosforge-common",        // shared header/topic constants, DLQ reason enum, base exceptions
    "chaosforge-avro-schemas",  // Avro codegen from src/main/avro (ADR-0525)
    "edge-gateway",             // WebFlux (Netty) — sole public ingress
    "control-plane",            // MVC + virtual threads — CAS replay, Spring AI
    "execution-service",        // MVC + virtual threads + @KafkaListener
)
