dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testImplementation("org.assertj:assertj-core:3.25.3")

    testImplementation("com.fasterxml.jackson.dataformat:jackson-dataformat-csv:2.16.1")
    testRuntimeOnly("ch.qos.logback:logback-classic:1.5.2")
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
