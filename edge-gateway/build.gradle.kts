// Edge Gateway — Spring WebFlux (Netty). The ONLY public ingress. Reactive end-to-end; no .block().
// No Spring AI (ADR-0518). No JDBC. Redis is for rate-limit buckets (Lettuce reactive), not L2 cache.
plugins {
    java
    alias(libs.plugins.springBoot)
    alias(libs.plugins.springDependencyManagement)
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    // Boot 4 reactive client builder (ClientHttpConnectorBuilder + HttpClientSettings) — not pulled by the
    // webflux starter; needed to attach the internal-mTLS SslBundle to the CP WebClient connector (ADR-0531).
    implementation("org.springframework.boot:spring-boot-http-client")
    // Boot 4 module split: WebClient.Builder autoconfig lives here now, not in webflux starter or core autoconfigure.
    implementation("org.springframework.boot:spring-boot-webclient")
    implementation("org.springframework.boot:spring-boot-starter-security")
    implementation("org.springframework.boot:spring-boot-starter-oauth2-resource-server")  // public JWT validation
    implementation("org.springframework.boot:spring-boot-starter-data-redis-reactive")     // rate-limit Lua buckets
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    // trace_id/MDC log correlation (rules-files mandate). Boot 4 module split: tracing autoconfig
    // is NOT pulled by the actuator starter — declare it explicitly. No exporter dep (ADR-0505).
    // Reactive MDC needs spring.reactor.context-propagation=auto (application.yml) on top of this.
    implementation("org.springframework.boot:spring-boot-micrometer-tracing-brave")
    implementation("io.micrometer:micrometer-tracing-bridge-brave")
    implementation("io.micrometer:micrometer-registry-prometheus")
    implementation(libs.resilience4j.spring.boot4)
    implementation(libs.resilience4j.reactor)        // ReactiveResilience4j CB on every upstream WebClient call
    implementation(libs.caffeine)                    // L1-only rule-set cache (ADR-0504)
    implementation(project(":chaosforge-common"))

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("io.projectreactor:reactor-test")
    testImplementation(libs.archunit.junit5)         // enforces: no .block() in edge-gateway/src/main
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation("org.testcontainers:junit-jupiter")
}
