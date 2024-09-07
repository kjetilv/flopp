import com.github.kjetilv.flopp.bld.Native
import com.github.kjetilv.flopp.bld.Native.runCommand
import org.gradle.jvm.toolchain.JvmVendorSpec.GRAAL_VM

dependencies {
    implementation(project(":flopp-kernel"))

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.11.0")
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:3.26.3")

    runtimeOnly("ch.qos.logback:logback-classic:1.5.6")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(22))
        vendor.set(GRAAL_VM)
    }
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
                ).map {
                    it.toString()
                },
                "com.github.kjetilv.flopp.lc.Lc",
                "lc",
                javaToolchains
            )
            command.also {
                project.logger.lifecycle("Running command: ")
                project.logger.lifecycle("  ${it.joinToString(" ")}")
                project.runCommand(command = it)
            }
        }
        doLast {
            val command = Native.image(
                listOf(
                    "../flopp-kernel/build/libs/flopp-kernel-${project.version}.jar",
                    "build/libs/flopp-lc-${project.version}.jar"
                ).map(
                    projectDir.toPath()::resolve
                ).map {
                    it.toString()
                },
                "com.github.kjetilv.flopp.ca.CalculateAverage_kjetilvlong",
                "ca",
                javaToolchains
            )
            command.also {
                project.logger.lifecycle("Running command: ")
                project.logger.lifecycle("  ${it.joinToString(" ")}")
                project.runCommand(command = it)
            }
        }
    }
//
