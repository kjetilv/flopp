plugins {
    java
    `maven-publish`
}

subprojects {
    apply(plugin = "maven-publish")
    apply(plugin = "java")

    group = "com.github.kjetilv.flopp"
    version = "0.1.0-SNAPSHOT"

    repositories {
        mavenLocal()
        mavenCentral()
    }

    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(22))
        }
        withSourcesJar()
        modularity.inferModulePath
        sourceCompatibility = JavaVersion.VERSION_22
        targetCompatibility = JavaVersion.VERSION_22
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
}


