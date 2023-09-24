import com.github.kjetilv.flopp.build.Native
import com.github.kjetilv.flopp.build.Native.runCommand

plugins {
    java
    `maven-publish`
}

group = "com.github.kjetilv.flopp"
version = "0.1.0-SNAPSHOT"

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    testRuntimeOnly("ch.qos.logback:logback-classic:1.4.7")
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
    testImplementation("org.assertj:assertj-core:3.24.2")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
        vendor.set(JvmVendorSpec.GRAAL_VM)
        withSourcesJar()
        languageVersion.set(JavaLanguageVersion.of(21))
        sourceCompatibility = JavaVersion.VERSION_21
        targetCompatibility = JavaVersion.VERSION_21
    }
}

tasks.test {
    useJUnitPlatform()
}

publishing {
    publications {
        register<MavenPublication>("floppPublication") {
            pom {
                name.set("Flopp")
                description.set("Flopp")
                url.set("https://github.com/kjetilv/flopp")

                licenses {
                    license {
                        name.set("GNU General Public License v3.0")
                        url.set("https://github.com/kjetilv/flopp/blob/main/LICENSE")
                    }
                }

                scm {
                    connection.set("scm:git:https://github.com/kjetilv/uplift")
                    developerConnection.set("scm:git:https://github.com/kjetilv/flopp")
                    url.set("https://github.com/kjetilv/flopp")
                }
            }
            from(components["java"])
        }
    }
}

tasks.register<Task>("native-image")
    .configure {
        dependsOn(tasks.named("jar").get())
        doLast {
            project.runCommand(
                command = Native.image(
                    "flopp-$version.jar",
                    "com.github.kjetilv.flopp.lc.Lc",
                    "lc",
                    javaToolchains
                )
            )
        }
    }
