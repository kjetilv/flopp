package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.ByteSource;
import com.github.kjetilv.flopp.kernel.ByteSources;
import com.github.kjetilv.flopp.kernel.Non;
import com.github.kjetilv.flopp.kernel.Partition;

import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Objects;

public final class FileChannelByteSources implements ByteSources {

    private final Path path;

    private final long size;

    private final int padding;

    public FileChannelByteSources(Path path, long size) {
        this(path, size, Math.toIntExact(Math.max(10L, size / 10)));
    }

    public FileChannelByteSources(Path path, long size, int padding) {
        this.path = Objects.requireNonNull(path, "path");
        this.size = Non.negative(size, "size");
        this.padding = Non.negative(padding, "padding");
    }

    @Override
    public ByteSource source(Partition partition) {
        return new FileChannelByteSource(partition, randomAccess(), size, padding);
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{path}]";
    }

    private RandomAccessFile randomAccess() {
        try {
            return new RandomAccessFile(path.toFile(), "r");
        } catch (Exception e) {
            throw new IllegalStateException(STR."Failed to open \{path}", e);
        }
    }
}
