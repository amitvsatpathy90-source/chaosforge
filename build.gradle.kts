plugins {
    java
    alias(libs.plugins.springBoot) apply false
    alias(libs.plugins.springDependencyManagement) apply false
    alias(libs.plugins.avro) apply false
}

allprojects {
    group = "io.chaosforge"
    version = "1.0.0"
}

subprojects {
    apply(plugin = "java")

    configure<JavaPluginExtension> {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(21))
        }
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
        options.compilerArgs.add("-parameters")   // method param names for Spring / Jackson 3
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        // Surface virtual-thread pinning during tests (architecture specifications JDBC threading; test asserts pinned == 0)
        jvmArgs("-Djdk.tracePinnedThreads=full")
    }
}
