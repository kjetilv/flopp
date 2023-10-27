package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.TempTargets;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileTempTargets implements TempTargets<Path> {

    private final Path tempDirectory;

    private final String name;

    private final int suffixIndex;

    private final String suffix;

    public FileTempTargets(Path source) {
        this.name = source.getFileName().toString();
        this.suffixIndex = name.lastIndexOf('.');
        this.suffix = suffixIndex < 0 ? "" : name.substring(suffixIndex + 1);
        this.tempDirectory = temp(source);
    }

    @Override
    public Path temp(Partition partition) {
        int no = partition.partitionNo();
        String tmpFilename = name + "-" + no + ".tmp" + (suffixIndex < 0 ? "" : "." + suffix);
        return tempDirectory.resolve(tmpFilename);
    }

    private Path temp(Path path) {
        try {
            String dir = "workdir-" + path.getFileName() + "-tmp";
            return Files.createTempDirectory(dir);
        } catch (IOException e) {
            throw new IllegalStateException(this + ": Failed to create temp dir", e);
        }
    }
}
