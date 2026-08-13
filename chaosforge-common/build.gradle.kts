// Shared, framework-light library: Kafka header/topic constants, DLQ reason enum, base exceptions.
// No Spring application context here. Keep dependencies minimal to avoid coupling the three surfaces.
plugins {
    `java-library`
    alias(libs.plugins.springDependencyManagement)
}

dependencyManagement {
    imports {
        mavenBom("org.springframework.boot:spring-boot-dependencies:${libs.versions.springBoot.get()}")
    }
}

dependencies {
    api(libs.jspecify)
    api(libs.uuid.creator)   // UUIDv7 helper shared by CP outbox + Exec
    implementation("org.slf4j:slf4j-api")

    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
