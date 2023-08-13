dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.3"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.3")
    testImplementation("org.assertj:assertj-core:3.26.0")

    testImplementation("com.fasterxml.jackson.dataformat:jackson-dataformat-csv:2.17.0")
    testRuntimeOnly("ch.qos.logback:logback-classic:1.5.6")
}
