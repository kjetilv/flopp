package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Shape;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Function;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

final class MemorySegmentSource implements Function<Partition, MemorySegment>, Closeable {

    private final Path path;

    private final RandomAccessFile randomAccessFile;

    private final Shape shape;

    private final Arena arena = Arena.ofAuto();

    private final FileChannel channel;

    MemorySegmentSource(Path path, Shape shape) {
        this.path = Objects.requireNonNull(path, "path");
        this.shape = Objects.requireNonNull(shape, "shape");
        this.randomAccessFile = openRandomAccess(path);
        this.channel = randomAccessFile.getChannel();
    }

    @Override
    public MemorySegment apply(Partition partition) {
        try {
            return channel.map(READ_ONLY, partition.offset(), partition.length(shape), arena);
        } catch (IOException e) {
            throw new IllegalStateException(STR."\{this} could not open [\{partition}", e);
        }
    }

    @Override
    public void close() {
        try {
            channel.close();
            randomAccessFile.close();
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed to close", e);
        }
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{path}]";
    }

    private RandomAccessFile openRandomAccess(Path path) {
        try {
            return new RandomAccessFile(path.toFile(), "r");
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} could not access file", e);
        }
    }
}
