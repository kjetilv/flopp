plugins {
    kotlin("jvm") version "2.0.20"
    `kotlin-dsl`
}

repositories {
    mavenCentral()
}

kotlin {
    jvmToolchain(22)
}
