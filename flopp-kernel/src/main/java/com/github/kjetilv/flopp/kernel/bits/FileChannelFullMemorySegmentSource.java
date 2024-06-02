package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.LineSegments;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Shape;

import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Objects;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

final class FileChannelFullMemorySegmentSource implements MemorySegmentSource {

    private final Path path;

    private final RandomAccessFile randomAccessFile;

    private final Shape shape;

    private final FileChannel channel;

    private final MemorySegment segment;

    FileChannelFullMemorySegmentSource(Path path, Shape shape) {
        this.path = Objects.requireNonNull(path, "path");
        this.randomAccessFile = openRandomAccess(path);
        this.shape = shape;
        this.channel = randomAccessFile.getChannel();
        try {
            Arena arena = Arena.ofAuto();
            segment = channel.map(READ_ONLY, 0, shape.size(), arena);
        } catch (Exception e) {
            throw new IllegalStateException(this + " could not open " + path, e);
        }
    }

    @Override
    public LineSegment get(Partition partition) {
        long length = partition.length(shape);
        if (length < 0) {
            throw new IllegalStateException("Invalid length " + length + ": " + partition);
        }
        return LineSegments.of(segment, partition);
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
