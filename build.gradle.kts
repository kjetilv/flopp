plugins {
    id("java")
    id("maven-publish")
}

group = "com.github.kjetilv.flopp"
version = "0.1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.slf4j:slf4j-api:2.0.7")

    testRuntimeOnly("ch.qos.logback:logback-classic:1.4.7")
    testImplementation(platform("org.junit:junit-bom:5.9.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.24.2")
}

configure<JavaPluginExtension> {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
    withSourcesJar()
}

//tasks.withType<JavaCompile> {
//    options.compilerArgs.add("--enable-preview")
//}

tasks.test {
    useJUnitPlatform()
}

publishing {
    publications {
        register<MavenPublication>("loppPublication") {
            pom {
                name.set("Lopp")
                description.set("Lopp")
                url.set("https://github.com/kjetilv/lopp")

                licenses {
                    license {
                        name.set("GNU General Public License v3.0")
                        url.set("https://github.com/kjetilv/lopp/blob/main/LICENSE")
                    }
                }

                scm {
                    connection.set("scm:git:https://github.com/kjetilv/uplift")
                    developerConnection.set("scm:git:https://github.com/kjetilv/lopp")
                    url.set("https://github.com/kjetilv/lopp")
                }
            }
            from(components["java"])
        }
    }
}
