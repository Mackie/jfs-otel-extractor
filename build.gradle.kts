import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    val kotlinVersion = "1.8.21"

    id("org.springframework.boot") version "3.1.1"
    id("io.spring.dependency-management") version "1.1.0"
    id("com.google.cloud.tools.jib") version "3.3.2"
    kotlin("jvm") version kotlinVersion
    kotlin("plugin.spring") version kotlinVersion
    kotlin("kapt") version kotlinVersion
}

group = "com.mackie.streams.otel"
version = "0.0.1-SNAPSHOT"
java.sourceCompatibility = JavaVersion.VERSION_17

val appName = providers.gradleProperty("application-name").get()
val ecrRepository = "${providers.gradleProperty("ecr-repository-root").get()}/$appName"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("io.projectreactor.kotlin:reactor-kotlin-extensions:1.1.7")
    implementation("io.micrometer:micrometer-registry-prometheus")
    implementation("io.github.microutils:kotlin-logging-jvm:2.1.23")

    implementation("org.springframework.kafka:spring-kafka:3.0.8")
    implementation("org.apache.kafka:kafka-streams:3.5.0")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.15.2")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

    implementation("com.github.ua-parser:uap-java:1.5.4")

    implementation ("org.rocksdb:rocksdbjni:8.3.2")

    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

    kapt("org.springframework.boot:spring-boot-configuration-processor")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.apache.kafka:kafka-streams-test-utils:3.5.0")
    testImplementation("org.mockito.kotlin:mockito-kotlin:5.0.0")
    kaptTest("org.springframework.boot:spring-boot-configuration-processor")
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs = listOf("-Xjsr305=strict")
        jvmTarget = "17"
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}

tasks.register("deploy") {
    dependsOn(tasks.jib)
    doLast {
        exec {
            commandLine(
                "helm",
                "upgrade",
                "--install",
                "--namespace",
                "pipeline",
                "--set",
                "image.repository=$ecrRepository",
                appName,
                "./charts"
            )
        }
    }
}

tasks.register("undeploy") {
    doLast {
        exec {
            commandLine(
                "helm",
                "uninstall",
                appName,
                "--namespace",
                "pipeline"
            )
        }
    }
}

jib {
    from {
        image = "amazoncorretto:17-alpine-jdk"
    }
    to {
        image = ecrRepository
        tags = setOf(project.version.toString(), "latest")
    }
}