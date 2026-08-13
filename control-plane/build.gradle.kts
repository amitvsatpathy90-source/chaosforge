// Control Plane — Spring MVC + Java 21 virtual threads. CRUD + CAS replay critical section (ADR-0522).
// Spring AI lives HERE ONLY (ADR-0518/0521). mTLS-gated inbound; JWT re-verified (ADR-0524).
plugins {
    java
    alias(libs.plugins.springBoot)
    alias(libs.plugins.springDependencyManagement)
}

dependencyManagement {
    imports {
        mavenBom("org.springframework.ai:spring-ai-bom:${libs.versions.springAi.get()}")
    }
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")                     // MVC (not webflux)
    implementation("org.springframework.boot:spring-boot-starter-security")
    implementation("org.springframework.boot:spring-boot-starter-oauth2-resource-server")  // JWT re-verify (ADR-0524)
    implementation("org.springframework.boot:spring-boot-starter-validation")              // @Valid AI-output gate
    implementation("org.springframework.boot:spring-boot-starter-data-jdbc")               // Spring Data JDBC + JdbcTemplate
    implementation("org.springframework.boot:spring-boot-starter-data-redis")              // L2 cache
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    // trace_id/MDC log correlation (rules-files mandate). Boot 4 module split: the actuator starter
    // does NOT pull tracing autoconfig — spring-boot-micrometer-tracing-brave must be explicit
    // (same lesson as spring-boot-flyway / spring-boot-http-client). No zipkin/otlp exporter dep:
    // spans stay in-process, MDC-only — ADR-0505 posture (no collector in the lab) unchanged.
    implementation("org.springframework.boot:spring-boot-micrometer-tracing-brave")
    implementation("io.micrometer:micrometer-tracing-bridge-brave")
    // Boot 4.1 split: @KafkaListener autoconfig, NOT pulled by spring-kafka alone
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    implementation("org.springframework.kafka:spring-kafka")                               // outbox relay producer
    // Boot 4 extracted per-tech autoconfig into separate modules: bare flyway-core no longer
    // auto-migrates. The starter pulls spring-boot-flyway (FlywayAutoConfiguration) so migrations
    // run — and fail-fast — at startup. (flyway-database-postgresql kept explicit for the PG dialect.)
    implementation("org.springframework.boot:spring-boot-starter-flyway")
    implementation("org.flywaydb:flyway-database-postgresql")
    implementation("io.micrometer:micrometer-registry-prometheus")
    implementation(libs.resilience4j.spring.boot4)
    implementation(libs.caffeine)                          // L1 of the two-level cache
    implementation(libs.springAi.starter.ollama)           // authoring + DLQ triage (CP only)
    implementation(libs.uuid.creator)                      // UUIDv7 outbox message_id
    implementation(project(":chaosforge-common"))
    implementation(project(":chaosforge-avro-schemas"))
    runtimeOnly("org.postgresql:postgresql")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.security:spring-security-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation(libs.archunit.junit5)               // no findById(UUID); no Mono/Flux; no advisory lock
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:postgresql")
    testImplementation("org.testcontainers:kafka")
}
