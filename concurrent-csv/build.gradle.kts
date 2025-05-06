plugins {
    // Apply the java-library plugin for API and implementation separation.
    `java-library`
    id("me.champeau.jmh") version "0.7.3"

}

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
}

dependencies {
    // Use JUnit Jupiter for testing.
    testImplementation(libs.junit.jupiter)

    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    // This dependency is exported to consumers, that is to say found on their compile classpath.
    api(libs.commons.math3)

    // This dependency is used internally, and not exposed to consumers on their own compile classpath.
    implementation(libs.guava)


    implementation("org.yaml:snakeyaml:2.4")
    implementation("software.amazon.awssdk:s3:2.31.35")
    implementation("org.reactivestreams:reactive-streams-flow-adapters:1.0.4")

    //implementation()
    // Add Project Reactor dependency
//    implementation(libs.reactor.core)

}

// Apply a specific Java toolchain to ease working on different environments.
java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

tasks.named<Test>("test") {
    // Use JUnit Platform for unit tests.
    useJUnitPlatform()
}

jmh {
    warmupIterations = 2
    iterations = 3
    fork = 1
}



tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("--enable-preview")
}

tasks.withType<Test>().configureEach {
    jvmArgs("--enable-preview")
}

tasks.withType<JavaExec>().configureEach {
    jvmArgs("--enable-preview")
}


