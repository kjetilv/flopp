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
        jarFiles: List<String>,
        mainClass: String,
        toBinary: String,
        javaToolchainService: JavaToolchainService,
        gc: String = "serial",
    ): List<String> =
        javaBin("native-image", javaToolchainService)?.let { nativeImage ->
            baseCommand(
                nativeImage,
                jarFiles.joinToString(":"),
                mainClass,
                toBinary,
                gc
            ).split(whitespace)
        } ?: throw IllegalStateException("Failed to resolve $toBinary")

    fun Project.runCommand(
        dir: File = libsDir,
        command: List<String>,
        fail: Boolean = true
    ) =
        exec {
            workingDir = dir
            commandLine = command.also {
                project.logger.info("Running command: \n ${it.joinToString("\n   ")}")
            }
        }.apply {
            if (fail) assertNormalExitValue()
        }.exitValue

    fun baseCommand(
        nativeImage: Path,
        fromJarFile: String,
        mainClass: String,
        toBinary: String,
        gc: String = "serial"
    ) =
        """
        $nativeImage -cp $fromJarFile $mainClass
         --verbose 
         --no-fallback
         --gc=$gc
         -H:Optimize=3
         -H:+ReportExceptionStackTraces
//         -H:+ForeignAPISupport
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
            languageVersion.set(JavaLanguageVersion.of(22))
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
