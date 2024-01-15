package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Transfer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

final class FileChannelTransfer implements Transfer {

    private final FileChannel channel;

    private final Partition partition;

    private final Path source;

    FileChannelTransfer(FileChannel channel, Partition partition, Path source) {
        this.channel = channel;
        this.partition = partition;
        this.source = source;
    }

    @Override
    public void run() {
        try (
            RandomAccessFile sourceFile = randomAccess(source);
            FileChannel sourceChannel = sourceFile.getChannel()
        ) {
            long totalTransferred = 0L;
            do {
                totalTransferred += channel.transferFrom(
                    sourceChannel,
                    partition.offset(),
                    partition.count()
                );
            } while (totalTransferred < partition.count());
        } catch (Exception e) {
            throw new IllegalStateException(STR."Failed to write \{source}", e);
        }
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{partition}]";
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new IllegalStateException(STR."\{this} failed to close \{channel}", e);
        }
    }

    private static RandomAccessFile randomAccess(Path path) {
        try {
            return new RandomAccessFile(path.toFile(), "r");
        } catch (Exception e) {
            throw new IllegalStateException(STR."Failed to open rw: \{path}", e);
        }
    }
}
