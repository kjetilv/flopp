package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Shape;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.util.Objects;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

public final class MemorySegmentHandler {

    private final long tailPos;

    private final int tail;

    private final Partition partition;

    private final FileChannel channel;

    private final Arena arena;

    private MemorySegment memorySegment;

    private final long offset;

    private final long length;

    public MemorySegmentHandler(Partition partition, Shape shape, FileChannel channel, Arena arena) {
        this.partition = Objects.requireNonNull(partition, "partition");
        this.channel = Objects.requireNonNull(channel, "channel");
        this.arena = Objects.requireNonNull(arena, "arena");
        Objects.requireNonNull(partition, "partition");
        Objects.requireNonNull(shape, "shape");

        offset = partition.offset();
        length = partition.length(shape);

        tail = Math.toIntExact(length % 8);
        tailPos = length - tail;
    }

    public MemorySegment memorySegment() {
        try {
            memorySegment = Objects.requireNonNull(channel, "channel")
                .map(READ_ONLY, offset, length, Objects.requireNonNull(arena, "arena"));
        } catch (IOException e) {
            throw new IllegalStateException(STR."\{this} could not open [\{offset}-\{length}] for \{partition}", e);
        }
        return memorySegment;
    }

    public long nextLong(long offset) {
        return memorySegment.get(ValueLayout.JAVA_LONG, offset);
    }

    public long tailLong() {
        long l = 0;
        for (int i = tail - 1; i >= 0; i--) {
            l <<= 8;
            l += memorySegment.get(ValueLayout.JAVA_BYTE, tailPos + i);
        }
        return l;
    }
}
