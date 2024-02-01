import com.github.kjetilv.flopp.bld.Native
import com.github.kjetilv.flopp.bld.Native.runCommand
import java.nio.file.Path

plugins {
    java
    `maven-publish`
}

dependencies {
    implementation(project(":flopp-kernel"))

    testRuntimeOnly("ch.qos.logback:logback-classic:1.4.12")
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
    testImplementation("org.assertj:assertj-core:3.24.2")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
        vendor.set(JvmVendorSpec.GRAAL_VM)
    }
    withSourcesJar()
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

tasks.test {
    useJUnitPlatform()
}

tasks.register<Task>("native-image")
    .configure {
        dependsOn(tasks.named("jar").get())
        doLast {
            val command = Native.image(
                listOf(
                    "../flopp-kernel/build/libs/flopp-kernel-${project.version}.jar",
                    "build/libs/flopp-lc-${project.version}.jar"
                ).map(
                    projectDir.toPath()::resolve
                ).map(
                    Path::toString
                ),
                "com.github.kjetilv.flopp.lc.Lc",
                "lc",
                javaToolchains
            )
            command.also {
                project.logger.lifecycle("Running command: ")
                project.logger.lifecycle("  ${it.joinToString(" ")}")
            }.also {
                project.runCommand(command = it)
            }
        }
    }
