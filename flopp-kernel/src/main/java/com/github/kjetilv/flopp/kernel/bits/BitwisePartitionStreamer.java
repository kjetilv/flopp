package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;

import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class BitwisePartitionStreamer implements PartitionedStreams.PartitionStreamer {

    private final Shape shape;

    private final MemorySegmentSource memorySegmentSource;

    private final Partition partition;

    public BitwisePartitionStreamer(Partition partition, Shape shape, MemorySegmentSource memorySegmentSource) {
        this.partition = Objects.requireNonNull(partition, "partition");
        this.shape = Objects.requireNonNull(shape, "shape");
        this.memorySegmentSource = Objects.requireNonNull(memorySegmentSource, "memorySegmentSource");
    }

    @Override
    public Partition partition() {
        return partition;
    }

    @Override
    public Stream<LineSegment> lines() {
        return StreamSupport.stream(
            new BitwisePartitionSpliterator(
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
