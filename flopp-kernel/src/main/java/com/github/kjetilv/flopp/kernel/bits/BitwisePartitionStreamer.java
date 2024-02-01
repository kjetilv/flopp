package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;

import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class BitwisePartitionStreamer implements PartitionedStreams.PartitionStreamer {

    private final Partition partition;

    private final BitwisePartitionSpliterator spliterator;

    public BitwisePartitionStreamer(
        Partition partition,
        Shape shape,
        MemorySegmentSource memorySegmentSource,
        BitwisePartitionStreamer next
    ) {
        this.partition = Objects.requireNonNull(partition, "partition");
        this.spliterator = new BitwisePartitionSpliterator(
            partition,
            memorySegmentSource,
            LineSegments.mediator(partition, shape),
            next == null ? null : next.spliterator
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
        return STR."\{getClass().getSimpleName()}[\{partition}]";
    }
}
