package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Shape;

import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Objects;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

final class FileChannelMemorySegmentSource implements MemorySegmentSource {

    private final Path path;

    private final RandomAccessFile randomAccessFile;

    private final Shape shape;

    private final Arena arena = Arena.ofAuto();

    private final FileChannel channel;

    FileChannelMemorySegmentSource(Path path, Shape shape) {
        this.path = Objects.requireNonNull(path, "path");
        this.shape = Objects.requireNonNull(shape, "shape");
        this.randomAccessFile = openRandomAccess(path);
        this.channel = randomAccessFile.getChannel();
    }

    @Override
    public Sourced get(Partition partition) {
        long length = partition.length(shape);
        if (length < 0) {
            throw new IllegalStateException("Invalid length " + length + ": " + partition);
        }
        try {
            return new Sourced(
                0L,
                channel.map(READ_ONLY, partition.offset(), length, arena)
            );
        } catch (Exception e) {
            throw new IllegalStateException(this + " could not open length " + length + ": " + partition, e);
        }
    }

    @Override
    public void close() {
        try {
            channel.close();
            randomAccessFile.close();
        } catch (Exception e) {
            throw new IllegalStateException(this + " failed to close", e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + path + "]";
    }

    private RandomAccessFile openRandomAccess(Path path) {
        try {
            return new RandomAccessFile(path.toFile(), "r");
        } catch (Exception e) {
            throw new IllegalStateException(this + " could not access file", e);
        }
    }
}
