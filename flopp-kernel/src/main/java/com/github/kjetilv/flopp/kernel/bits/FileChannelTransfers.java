package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Transfers;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Objects;

final class FileChannelTransfers implements Transfers<Path> {

    private final String target;

    private final RandomAccessFile randomAccessFile;

    private final FileChannel receivingChannel;

    FileChannelTransfers(Path target) {
        this.randomAccessFile = randomAccess(Objects.requireNonNull(target, "path"));
        this.receivingChannel = this.randomAccessFile.getChannel();
        this.target = target.toString();
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
            throw new IllegalStateException(this + " failed to close " + receivingChannel + "/" + randomAccessFile, e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + target + "]";
    }

    private RandomAccessFile randomAccess(Path file) {
        try {
            return new RandomAccessFile(file.toFile(), READ_WRITE);
        } catch (Exception e) {
            throw new IllegalStateException(this + " failed to open " + file, e);
        }
    }

    private static final String READ_WRITE = "rw";
}
