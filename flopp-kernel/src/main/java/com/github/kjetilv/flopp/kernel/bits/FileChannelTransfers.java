package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Transfer;
import com.github.kjetilv.flopp.kernel.Transfers;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Objects;

final class FileChannelTransfers implements Transfers<Path> {

    private final Path target;

    private final RandomAccessFile randomAccessFile;

    private final FileChannel receivingChannel;

    FileChannelTransfers(Path target) {
        this.target = Objects.requireNonNull(target, "path");
        this.randomAccessFile = randomAccess(this.target);
        this.receivingChannel = this.randomAccessFile.getChannel();
    }

    @Override
    public Transfer transfer(Partition partition, Path source) {
        return new FileChannelTransfer(receivingChannel, partition, source);
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

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{target}]";
    }

    private static RandomAccessFile randomAccess(Path file) {
        try {
            return new RandomAccessFile(file.toFile(), "rw");
        } catch (Exception e) {
            throw new IllegalStateException(STR."Failed to open \{file}", e);
        }
    }
}
