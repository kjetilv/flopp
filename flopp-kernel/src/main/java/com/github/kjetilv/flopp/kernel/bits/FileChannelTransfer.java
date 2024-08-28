package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

final class FileChannelTransfer implements Transfer {

    private final FileChannel receivingChannel;

    private final Partition partition;

    private final Path source;

    FileChannelTransfer(
        FileChannel receivingChannel,
        Partition partition,
        Path source
    ) {
        this.receivingChannel = receivingChannel;
        this.partition = partition;
        this.source = source;
    }

    @Override
    public void run() {
        try (
            RandomAccessFile sourceFile = randomAccess(source);
            FileChannel donorChannel = sourceFile.getChannel()
        ) {
            long totalTransferred = 0L;
            do {
                totalTransferred += receivingChannel.transferFrom(
                    donorChannel,
                    partition.offset(),
                    partition.length()
                );
            } while (totalTransferred < partition.length());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to write " + source, e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + partition + "]";
    }

    @Override
    public void close() {
        try {
            receivingChannel.close();
        } catch (IOException e) {
            throw new IllegalStateException(this + " failed to close " + receivingChannel, e);
        }
    }

    private RandomAccessFile randomAccess(Path path) {
        try {
            return new RandomAccessFile(path.toFile(), "r");
        } catch (Exception e) {
            throw new IllegalStateException(this + " failed to open rw: " + path, e);
        }
    }
}
