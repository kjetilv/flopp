package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.partitions.Partition;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

final class FileChannelTransfers implements Transfers<Path> {

    private final Path target;

    private final RandomAccessFile randomAccessFile;

    private final FileChannel receivingChannel;

    FileChannelTransfers(Path target) {
        this.target = Objects.requireNonNull(target, "target");
        this.randomAccessFile = randomAccess(Objects.requireNonNull(this.target, "path"));
        this.receivingChannel = this.randomAccessFile.getChannel();
    }

    @Override
    public Transfer transfer(Partition partition, Path source) {
        return new FileChannelTransfer(receivingChannel, partition, source);
    }

    @Override
    public void close() {
        try {
            receivingChannel.close();
        } catch (Exception e) {
            throw new IllegalStateException(this + " failed to close " + receivingChannel, e);
        }
        try {
            randomAccessFile.close();
        } catch (Exception e) {
            throw new IllegalStateException(this + " failed to close " + randomAccessFile, e);
        }
    }

    private RandomAccessFile randomAccess(Path file) {
        if (Files.isDirectory(file)) {
            throw new IllegalArgumentException("Not a file: " + file);
        }
        try {
            if (!Files.exists(file)) {
                Files.createFile(file);
            }
            return new RandomAccessFile(file.toAbsolutePath().toFile(), READ_WRITE);
        } catch (Exception e) {
            throw new IllegalStateException(this + " failed to open " + file, e);
        }
    }

    private static final String READ_WRITE = "rw";

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + target + "]";
    }
}
