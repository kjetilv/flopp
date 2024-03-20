dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testImplementation("org.assertj:assertj-core:3.25.3")

    testImplementation("com.fasterxml.jackson.dataformat:jackson-dataformat-csv:2.16.1")
    testRuntimeOnly("ch.qos.logback:logback-classic:1.5.2")
}

