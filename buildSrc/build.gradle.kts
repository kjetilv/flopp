plugins {
    kotlin("jvm") version "2.0.0"
    `kotlin-dsl`
}

repositories {
    mavenCentral()
}

kotlin {
    jvmToolchain(22)
}
