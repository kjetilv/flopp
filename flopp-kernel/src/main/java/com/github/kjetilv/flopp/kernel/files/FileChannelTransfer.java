package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Transfer;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

final class FileChannelTransfer implements Transfer {

    private final RandomAccessFile randomAccessFile;

    private final Partition partition;

    private final Path result;

    FileChannelTransfer(RandomAccessFile randomAccessFile, Partition partition, Path result) {
        this.randomAccessFile = randomAccessFile;
        this.partition = partition;
        this.result = result;
    }

    @Override
    public void run() {
        try (FileChannel channel = randomAccessFile.getChannel()) {
            long totalTransferred = 0L;
            do {
                totalTransferred += channel.transferFrom(
                    channel,
                    partition.offset(),
                    partition.count()
                );
            } while (totalTransferred < partition.count());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to write " + result, e);
        }
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{randomAccessFile}]";
    }

    private static RandomAccessFile randomAccess(Path path) {
        try {
            return new RandomAccessFile(path.toFile(), "r");
        } catch (Exception e) {
            throw new IllegalStateException(STR."Failed to open rw: \{path}", e);
        }
    }
}
