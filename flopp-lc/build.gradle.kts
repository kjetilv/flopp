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
        languageVersion.set(JavaLanguageVersion.of(23))
    }
}

