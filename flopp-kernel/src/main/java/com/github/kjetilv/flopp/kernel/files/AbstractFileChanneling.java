package com.github.kjetilv.flopp.kernel.files;

import java.io.Closeable;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Objects;

abstract sealed class AbstractFileChanneling implements Closeable
    permits FileChannelByteSources, FileChannelTransfers {

    private final Path path;

    protected AbstractFileChanneling(Path path, boolean writable) {
        this.path = Objects.requireNonNull(path, "target");
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{path}]";
    }

    protected RandomAccessFile randomAccess() {
        return randomAccess(false);
    }

    protected RandomAccessFile randomAccess(boolean writable) {
        String mode = writable ? "rw" : "r";
        try {
            return new RandomAccessFile(path.toFile(), mode);
        } catch (Exception e) {
            throw new IllegalStateException(STR."Failed to open in `\{mode}`: \{path}", e);
        }
    }
}
