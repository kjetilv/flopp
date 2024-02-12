plugins {
    java
    `maven-publish`
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testImplementation("org.assertj:assertj-core:3.25.3")

    testImplementation("com.fasterxml.jackson.dataformat:jackson-dataformat-csv:2.16.1")
    testRuntimeOnly("ch.qos.logback:logback-classic:1.4.14")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
    withSourcesJar()
    modularity.inferModulePath
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

tasks {
    withType<JavaCompile> {
        options.compilerArgs.add("--enable-preview")
    }
    withType<Test>() {
        jvmArgs("--enable-preview")
        useJUnitPlatform()
    }
    withType<JavaExec>() {
        jvmArgs("--enable-preview")
    }
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
