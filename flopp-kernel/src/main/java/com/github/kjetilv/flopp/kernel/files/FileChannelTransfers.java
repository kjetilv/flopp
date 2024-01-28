package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Transfer;
import com.github.kjetilv.flopp.kernel.Transfers;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Objects;

public final class FileChannelTransfers implements Transfers<Path> {

    private final Path target;

    private final RandomAccessFile randomAccessFile;

    private final FileChannel receivingChannel;

    public FileChannelTransfers(Path target) {
        this.target = Objects.requireNonNull(target, "path");
        this.randomAccessFile = randomAccess(this.target);
        this.receivingChannel = this.randomAccessFile.getChannel();
    }

    @Override
    public Transfer transfer(Partition partition, Path source) {
        return new FileChannelTransfer(receivingChannel, partition, source);
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{target}]";
    }

    @Override
    public void close() {
        try {
            try {
                receivingChannel.close();
            } finally {
                randomAccessFile.close();
            }
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed to close \{receivingChannel}/\{randomAccessFile}", e);
        }
    }

    private static RandomAccessFile randomAccess(Path file) {
        try {
            return new RandomAccessFile(file.toFile(), "rw");
        } catch (Exception e) {
            throw new IllegalStateException(STR."Failed to open \{file}", e);
        }
    }
}
