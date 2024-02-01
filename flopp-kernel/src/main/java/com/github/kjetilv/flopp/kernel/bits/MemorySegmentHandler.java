package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Partitioning;
import com.github.kjetilv.flopp.kernel.Shape;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;
import java.util.function.Supplier;

public final class MemorySegmentHandler {

    private final long tailPos;

    private final int tail;

    private final Supplier<MemorySegment> memorySegmentSupplier;

    private MemorySegment memorySegment;

    public MemorySegmentHandler(
        Partition partition,
        Shape shape,
        Supplier<MemorySegment> memorySegmentSupplier
    ) {
        Objects.requireNonNull(partition, "partition");
        this.memorySegmentSupplier =
            Objects.requireNonNull(memorySegmentSupplier, "memorySegmentSupplier");
        long length = partition.length(shape);
        tail = Math.toIntExact(length % Partitioning.ALIGNMENT);
        tailPos = length - tail;
    }

    public MemorySegment memorySegment() {
        return memorySegment = memorySegmentSupplier.get();
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
