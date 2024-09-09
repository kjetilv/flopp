package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.TempTargets;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.stream.Stream;

class TempDirTargets implements TempTargets<Path> {

    private final Path tempDirectory;

    private final String sourceName;

    private final int suffixIndex;

    private final String suffix;

    TempDirTargets(Path base) {
        this(Objects.requireNonNull(base, "base").getFileName().toString());
    }

    TempDirTargets(String fileName) {
        this.sourceName = fileName;
        this.suffixIndex = sourceName.lastIndexOf('.');
        this.suffix = suffixIndex < 0 ? "" : sourceName.substring(suffixIndex + 1);
        String prefix = "workdir-" + fileName + "-tmp";
        try {
            this.tempDirectory = Files.createTempDirectory(prefix);
        } catch (IOException e) {
            throw new IllegalStateException(this + " Failed to create temp dir " + prefix, e);
        }
    }

    @Override
    public Path temp(Partition partition) {
        int no = partition.partitionNo();
        String tmpFilename = sourceName + "-" + no + ".tmp." + (suffixIndex < 0 ? "" : suffix);
        return tempDirectory.resolve(tmpFilename);
    }

    @Override
    public void close() {
        try {
            try (Stream<Path> list = Files.list(tempDirectory)) {
                list.forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        throw new IllegalStateException("Failed to delete " + path, e);
                    }
                });
            }
            Files.deleteIfExists(tempDirectory);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to delete " + tempDirectory, e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + tempDirectory + "]";
    }
}
