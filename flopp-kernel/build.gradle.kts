dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.11.0")
    testImplementation("org.assertj:assertj-core:3.26.3")
    testImplementation(platform("org.junit:junit-bom:5.11.0"))
    testImplementation("com.fasterxml.jackson.dataformat:jackson-dataformat-csv:2.17.2")

    testRuntimeOnly("ch.qos.logback:logback-classic:1.5.6")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter:5.11.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter:5.11.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
