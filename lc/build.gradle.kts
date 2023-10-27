import com.github.kjetilv.flopp.bld.Native
import com.github.kjetilv.flopp.bld.Native.runCommand
import java.nio.file.Paths

plugins {
    java
    `maven-publish`
}

dependencies {
    implementation(project(":kernel"))

    testRuntimeOnly("ch.qos.logback:logback-classic:1.4.7")
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
            project.runCommand(
                command = Native.image(
                    "/Users/kjetilvalstadsve/Development/git/flopp/kernel/build/libs/flopp-0.1.0-SNAPSHOT.jar:/Users/kjetilvalstadsve/Development/git/flopp/lc/build/libs/lc-0.1.0-SNAPSHOT.jar:",
                    "com.github.kjetilv.flopp.lc.Lc",
                    "lc",
                    javaToolchains
                )
            )
        }
    }
