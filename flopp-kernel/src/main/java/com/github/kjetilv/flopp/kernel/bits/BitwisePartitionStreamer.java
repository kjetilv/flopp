package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.PartitionStreamer;
import com.github.kjetilv.flopp.kernel.Shape;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

final class BitwisePartitionStreamer implements PartitionStreamer {

    private final Partition partition;

    private final BitwisePartitionSpliterator spliterator;

    BitwisePartitionStreamer(
        Partition partition,
        Shape shape,
        MemorySegmentSource memorySegmentSource,
        Supplier<BitwisePartitionStreamer> next,
        boolean immutable
    ) {
        this.partition = partition;
        MemorySegment memorySegment = memorySegmentSource.get(partition);
        long logicalSize = memorySegment.byteSize();
        MemorySegment safeSegment = partition.last()
            ? MemorySegments.alignmentPadded(memorySegment)
            : memorySegment;
        this.spliterator = new BitwisePartitionSpliterator(
            partition,
            safeSegment,
            logicalSize,
            HeadersAndFooters.middleMan(partition, shape),
            next == null ? null : () -> next.get().spliterator,
            immutable
        );
    }

    @Override
    public Stream<LineSegment> lines() {
        return StreamSupport.stream(spliterator, false);
    }

    @Override
    public Partition partition() {
        return partition;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + partition + "]";
    }
}
