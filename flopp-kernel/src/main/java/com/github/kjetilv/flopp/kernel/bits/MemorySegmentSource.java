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

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

public final class MemorySegmentSource implements Closeable {

    private final Path path;

    private final RandomAccessFile randomAccessFile;

    private final Shape shape;

    private final Arena arena;

    private final FileChannel channel;

    public MemorySegmentSource(Path path, Shape shape, Arena arena) {
        this.path = Objects.requireNonNull(path, "path");
        this.shape = Objects.requireNonNull(shape, "shape");
        this.arena = Objects.requireNonNull(arena, "arena");

        this.randomAccessFile = openRandomAccess(path);
        this.channel = randomAccessFile.getChannel();
    }

    public MemorySegment open(Partition partition) {
        long offset = partition.offset();
        long length = length(partition);
        try {
            return channel.map(READ_ONLY, offset, length, arena);
        } catch (IOException e) {
            throw new IllegalStateException(STR."\{this} could not open [\{offset}-\{length}] for \{partition}", e);
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
        return STR."\{getClass().getSimpleName()}[\{path}]";
    }

    private RandomAccessFile openRandomAccess(Path path) {
        try {
            return new RandomAccessFile(path.toFile(), "r");
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} could not access file", e);
        }
    }

    private long length(Partition partition) {
        if (shape.limitsLineLength()) {
            return Math.min(
                shape.size() - partition.offset(),
                partition.bufferedTo(shape.stats().longestLine() + 1)
            );
        }
        throw new IllegalStateException(STR."Shape does not specify max line length: \{shape}");
    }}
