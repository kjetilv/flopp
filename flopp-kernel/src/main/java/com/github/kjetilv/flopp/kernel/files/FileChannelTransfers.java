package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Transfer;
import com.github.kjetilv.flopp.kernel.Transfers;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Objects;

final class FileChannelTransfers implements Transfers<Path> {

    private final Path path;

    private final RandomAccessFile randomAccessFile;

    private final FileChannel channel;

    FileChannelTransfers(Path path) {
        this.path = Objects.requireNonNull(path, "path");
        this.randomAccessFile = randomAccess();
        channel = this.randomAccessFile.getChannel();
    }

    @Override
    public Transfer transfer(Partition partition, Path source) {
        return new FileChannelTransfer(channel, partition, source);
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{path}]";
    }

    @Override
    public void close() {
        try {
            try {
                channel.close();
            } finally {
                randomAccessFile.close();
            }
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed to close \{channel}/\{randomAccessFile}", e);
        }
    }

    private RandomAccessFile randomAccess() {
        try {
            return new RandomAccessFile(path.toFile(), "rw");
        } catch (Exception e) {
            throw new IllegalStateException(STR."Failed to open \{path}", e);
        }
    }
}
