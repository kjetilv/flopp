package com.github.kjetilv.flopp.kernel.files;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Objects;

abstract sealed class FileChannelBase implements Closeable
    permits FileChannelSources, FileChannelTransfers {

    private final Path target;

    private final RandomAccessFile randomAccessFile;

    private final FileChannel channel;

    protected FileChannelBase(Path target, boolean writable) {
        this.target = Objects.requireNonNull(target, "target");
        this.randomAccessFile = randomAccess(this.target, writable ? "rw" : "r");
        this.channel = randomAccessFile.getChannel();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + target + "]";
    }

    @Override
    public void close() {
        try {
            this.channel.close();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to close: " + channel, e);
        }
        try {
            randomAccessFile.close();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to close: " + randomAccessFile, e);
        }
    }

    protected FileChannel channel() {
        return channel;
    }

    protected static RandomAccessFile randomAccess(Path path, String mode) {
        try {
            return new RandomAccessFile(path.toFile(), mode);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to open " + "rw" + ": " + path, e);
        }
    }
}
