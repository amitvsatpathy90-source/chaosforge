// Execution Service — Spring MVC + virtual threads + @KafkaListener. Consumer pipeline is load-bearing.
// NO Spring AI (ADR-0518 — ArchUnit enforces no ChatClient bean). mTLS-gated inbound.
plugins {
    java
    alias(libs.plugins.springBoot)
    alias(libs.plugins.springDependencyManagement)
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")                     // MVC admin endpoints
    // Boot 4 client builders (ClientHttpRequestFactoryBuilder + HttpClientSettings) — not pulled by the
    // web starter; needed to attach the internal-mTLS SslBundle to the CP RestClient (ADR-0531).
    implementation("org.springframework.boot:spring-boot-http-client")
    implementation("org.springframework.boot:spring-boot-starter-security")
    implementation("org.springframework.boot:spring-boot-starter-oauth2-resource-server")  // JWT on admin HTTP path
    implementation("org.springframework.boot:spring-boot-starter-jdbc")                    // platform-thread JDBC
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    // trace_id/MDC log correlation (rules-files mandate). Boot 4 module split: tracing autoconfig
    // is NOT pulled by the actuator starter — declare it explicitly. No exporter dep (ADR-0505).
    implementation("org.springframework.boot:spring-boot-micrometer-tracing-brave")
    implementation("io.micrometer:micrometer-tracing-bridge-brave")
    // Boot 4.1 split: @KafkaListener autoconfig, NOT pulled by spring-kafka alone
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    implementation("org.springframework.kafka:spring-kafka")                               // @KafkaListener + DLQ
    // Boot 4 extracted per-tech autoconfig into separate modules: bare flyway-core no longer
    // auto-migrates. The starter pulls spring-boot-flyway (FlywayAutoConfiguration) so migrations
    // run — and fail-fast — at startup. (flyway-database-postgresql kept explicit for the PG dialect.)
    implementation("org.springframework.boot:spring-boot-starter-flyway")
    implementation("org.flywaydb:flyway-database-postgresql")
    implementation("io.micrometer:micrometer-registry-prometheus")
    implementation(libs.resilience4j.spring.boot4)         // per-step-call CB/bulkhead/timeout
    implementation(libs.caffeine)                          // L1 rule-set cache (append-only; ADR-0503)
    implementation(libs.uuid.creator)                      // UUIDv7 result-event message_id
    implementation(project(":chaosforge-common"))
    implementation(project(":chaosforge-avro-schemas"))
    runtimeOnly("org.postgresql:postgresql")

    // Deliberately NO spring-ai-* dependency here (ADR-0518).

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.security:spring-security-test")   // jwt()/authorities() MockMvc
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation(libs.archunit.junit5)               // no ChatClient; no Mono/Flux; fencing-before-inbox
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:postgresql")
    testImplementation("org.testcontainers:kafka")
}
