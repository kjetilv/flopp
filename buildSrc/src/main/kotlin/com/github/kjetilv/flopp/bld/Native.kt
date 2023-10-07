package com.github.kjetilv.flopp.bld

import org.gradle.api.Project
import org.gradle.jvm.toolchain.JavaLanguageVersion
import org.gradle.jvm.toolchain.JavaToolchainService
import org.gradle.jvm.toolchain.JvmVendorSpec
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

object Native {

    fun image(
        fromJarFile: String,
        mainClass: String,
        toBinary: String,
        javaToolchainService: JavaToolchainService,
    ): List<String> =
        javaBin("native-image", javaToolchainService)?.let { nativeImage ->
            baseCommand(nativeImage, fromJarFile, mainClass, toBinary).split(whitespace)
        } ?: throw IllegalStateException("Failed to resolve $toBinary")

    fun Project.runCommand(
        dir: File = libsDir,
        command: List<String>,
        fail: Boolean = true
    ) =
        exec {
            workingDir = dir
            commandLine = command
        }.apply { if (fail) assertNormalExitValue() }.exitValue

    fun baseCommand(nativeImage: Path, fromJarFile: String, mainClass: String, toBinary: String) =
        """
        $nativeImage -cp $fromJarFile $mainClass
         --verbose 
         --no-fallback
         -H:+ReportExceptionStackTraces
         -o $toBinary
         -march=native
        """.trimIndent()

    private val Project.libsDir: File
        get() =
            layout.buildDirectory.dir("libs").get().asFile.also {
                Files.createDirectories(it.toPath())
            }

    fun javaBin(binary: String, javaToolchainService: JavaToolchainService): Path? {
        return javaToolchainService.compilerFor {
            vendor.set(JvmVendorSpec.GRAAL_VM)
            languageVersion.set(JavaLanguageVersion.of(21))
        }.map {
            it.executablePath
        }.map {
            it.asFile
        }.map {
            it.toPath()
        }.map {
            it.parent
        }.orNull?.resolve(binary)
    }

    private val String.sysprop get() = System.getProperty(this)

    private val String.asPath get() = Paths.get(this)

    private val whitespace = "\\s+".toRegex()
}
