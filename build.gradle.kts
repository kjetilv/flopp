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
            repositories {
                mavenLocal()
                maven {
                    name = "GitHubPackages"
                    url = uri("https://maven.pkg.github.com/kjetilv/uplift")
                    credentials {
                        username = resolveProperty("githubUser", "GITHUB_ACTOR")
                        password = resolveProperty("githubToken", "GITHUB_TOKEN")
                    }
                }
            }

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

fun resolveProperty(property: String, variable: String? = null, defValue: String? = null) =
    System.getProperty(property)
        ?: variable?.let { System.getenv(it) }
        ?: project.property(property)?.toString()
        ?: defValue
        ?: throw IllegalStateException("No variable $property found, no default value provided")
