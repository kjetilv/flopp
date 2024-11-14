package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Partition;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

class TempDirTargets implements TempTargets<Path> {

    private final AtomicReference<Path> tempDirectory = new AtomicReference<>();

    private final String sourceName;

    private final int suffixIndex;

    private final String suffix;

    private final String prefix;

    TempDirTargets(Path base) {
        this.sourceName = Objects.requireNonNull(base, "base").getFileName().toString();
        this.suffixIndex = this.sourceName.lastIndexOf('.');
        this.suffix = suffixIndex < 0 ? "" : this.sourceName.substring(suffixIndex + 1);
        this.prefix = "workdir-" + this.sourceName + "-tmp";
    }

    @Override
    public Path temp(Partition partition) {
        int no = partition.partitionNo();
        String tmpFilename = sourceName + "-" + no + ".tmp." + (suffixIndex < 0 ? "" : suffix);
        return tempDirectory().resolve(tmpFilename);
    }

    @Override
    public void close() {
        Path tmp = tempDirectory.get();
        if (tmp == null) {
            return;
        }
        try {
            try (Stream<Path> list = Files.list(tmp)) {
                list.forEach(file -> {
                    try {
                        Files.delete(file);
                    } catch (IOException e) {
                        throw new IllegalStateException("Failed to delete " + file, e);
                    }
                });
            }
            Files.deleteIfExists(tmp);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to delete " + tmp, e);
        } finally {
            tempDirectory.set(null);
        }
    }

    private Path tempDirectory() {
        return tempDirectory.updateAndGet(tmp -> {
            if (tmp == null) {
                try {
                    return Files.createTempDirectory(prefix);
                } catch (IOException e) {
                    throw new IllegalStateException(this + " Failed to create temp dir " + prefix, e);
                }
            }
            return tmp;
        });
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + tempDirectory.get() + "]";
    }
}
