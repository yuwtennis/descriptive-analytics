import org.gradle.internal.impldep.org.apache.commons.io.output.ByteArrayOutputStream

/*
 * This file was generated by the Gradle 'init' task.
 *
 * This generated file contains a sample Java application project to get you started.
 * For more details on building Java & JVM projects, please refer to https://docs.gradle.org/8.8/userguide/building_java_projects.html in the Gradle documentation.
 */

version = "v0.0.1"
val repositoryUrl: String = System.getenv("DOCKER_REPOS_URL") ?: "myrepos"
val dockerTag: String = System.getenv("GIT_REV") ?: "${project.version}"

plugins {
    id("com.palantir.docker") version "0.36.0"

    // Apply the application plugin to add support for building a CLI application in Java.
    application
}

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
}

dependencies {
    // Use JUnit Jupiter for testing.
    testImplementation(libs.junit.jupiter)

    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    // This dependency is used by the application.
    implementation(libs.guava)
}

// Apply a specific Java toolchain to ease working on different environments.
java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

docker {
    dependsOn(tasks.distTar.get())
    name = "${rootProject.name}:${dockerTag}"
    tag("RemoteRepos", "${repositoryUrl}:${dockerTag}")
    files(tasks.distTar.get().archiveFile)
    buildArgs(mapOf("ARTIFACT_NAME" to tasks.distTar.get().archiveFileName.get()))
}

application {
    // Define the main class for the application.
    mainClass = "org.example.App"
}

tasks.named<Test>("test") {
    // Use JUnit Platform for unit tests.
    useJUnitPlatform()
}
