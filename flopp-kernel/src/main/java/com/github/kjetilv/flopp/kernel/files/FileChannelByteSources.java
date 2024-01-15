package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.ByteSource;
import com.github.kjetilv.flopp.kernel.ByteSources;
import com.github.kjetilv.flopp.kernel.Non;
import com.github.kjetilv.flopp.kernel.Partition;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Objects;

public final class FileChannelByteSources implements ByteSources {

    private final Path path;

    private final long size;

    private final int padding;

    private final RandomAccessFile randomAccessFile;

    private final FileChannel channel;

    public FileChannelByteSources(Path path, long size) {
        this(path, size, Math.toIntExact(Math.max(10L, size / 10)));
    }

    public FileChannelByteSources(Path path, long size, int padding) {
        this.path = Objects.requireNonNull(path, "path");
        this.size = Non.negative(size, "size");
        this.padding = Non.negative(padding, "padding");
        this.randomAccessFile = randomAccess();
        this.channel = this.randomAccessFile.getChannel();
    }

    @Override
    public ByteSource source(Partition partition) {
        return new FileChannelByteSource(partition, channel, size, padding);
    }

    @Override
    public void close() {
        try {
            this.channel.close();
        } catch (IOException e) {
            throw new IllegalStateException(STR."\{this} failed to close: \{channel}", e);
        }
        try {
            randomAccessFile.close();
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed to close: \{randomAccessFile}", e);
        }

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
