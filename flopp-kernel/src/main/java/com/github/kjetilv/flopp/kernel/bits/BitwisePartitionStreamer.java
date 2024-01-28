package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Mediator;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Shape;

import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class BitwisePartitionStreamer {

    private final Shape shape;

    private final MemorySegmentSource memorySegmentSource;

    private final Partition partition;

    public BitwisePartitionStreamer(Partition partition, Shape shape, MemorySegmentSource memorySegmentSource) {
        this.partition = Objects.requireNonNull(partition, "partition");
        this.shape = Objects.requireNonNull(shape, "shape");
        this.memorySegmentSource = Objects.requireNonNull(memorySegmentSource, "memorySegmentSource");
    }

    public Partition partition() {
        return partition;
    }

    public Stream<LineSegment> lines() {
        return StreamSupport.stream(
            new BitwisePartitionSpliterator2(
                partition,
                memorySegmentSource,
                LineSegments.mediator(partition, shape)
            ),
            false
        );
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{partition}]";
    }
}
