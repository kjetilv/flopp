import org.gradle.api.JavaVersion.VERSION_23

plugins {
    java
    `maven-publish`
}

subprojects {
    apply(plugin = "maven-publish")
    apply(plugin = "java")
    apply(plugin = "jvm-test-suite")

    group = "com.github.kjetilv.flopp"
    version = "0.1.0-SNAPSHOT"

    repositories {
        mavenLocal()
        mavenCentral()
    }

    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(23))
        }
        withSourcesJar()
        modularity.inferModulePath
        sourceCompatibility = VERSION_23
        targetCompatibility = VERSION_23
    }


    tasks.named<Test>("test") {
        useJUnitPlatform()
    }

    tasks.withType<JavaCompile>().all {
        options.compilerArgs.add("--enable-preview")
    }

    tasks.withType<Test>().all {
        jvmArgs("--enable-preview")
    }

    tasks.withType<JavaExec>().configureEach {
        jvmArgs("--enable-preview")
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
                        connection.set("scm:git:https://github.com/kjetilv/flopp")
                        developerConnection.set("scm:git:https://github.com/kjetilv/flopp")
                        url.set("https://github.com/kjetilv/flopp")
                    }
                }
                from(components["java"])
            }
        }
    }
}
