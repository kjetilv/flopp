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
            options.forkOptions.jvmArgs!!.add("--enable-preview")
        }
        withType<Test> {
            jvmArgs("--enable-preview")
            useJUnitPlatform()
        }
        withType<JavaExec> {
            jvmArgs("--enable-preview")
        }
    }
}


