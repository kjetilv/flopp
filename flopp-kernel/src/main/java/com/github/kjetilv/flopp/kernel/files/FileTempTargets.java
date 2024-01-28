package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.TempTargets;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileTempTargets implements TempTargets<Path> {

    private final Path tempDirectory;

    private final String sourceName;

    private final int suffixIndex;

    private final String suffix;

    public FileTempTargets(Path source) {
        this.sourceName = source.getFileName().toString();
        this.suffixIndex = sourceName.lastIndexOf('.');
        this.suffix = suffixIndex < 0 ? "" : sourceName.substring(suffixIndex + 1);
        this.tempDirectory = temp(source);
    }

    @Override
    public Path temp(Partition partition) {
        int no = partition.partitionNo();
        String tmpFilename = STR."\{sourceName}-\{no}.tmp\{suffixIndex < 0 ? "" : STR.".\{suffix}"}";
        return tempDirectory.resolve(tmpFilename);
    }

    @Override
    public void close() {
        try {
            Files.deleteIfExists(tempDirectory);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to close", e);
        }
    }

    private Path temp(Path source) {
        try {
            String dir = STR."workdir-\{source.getFileName()}-tmp";
            return Files.createTempDirectory(dir);
        } catch (IOException e) {
            throw new IllegalStateException(STR."\{this}: Failed to create temp dir", e);
        }
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{tempDirectory}]";
    }
}
