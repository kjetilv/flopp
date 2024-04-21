package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.PartitionStreamer;
import com.github.kjetilv.flopp.kernel.Shape;

import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

final class BitwisePartitionStreamer implements PartitionStreamer {

    private final Partition partition;

    private final BitwisePartitionSpliterator spliterator;

    BitwisePartitionStreamer(
        Partition partition,
        Shape shape,
        MemorySegmentSource memorySegmentSource,
        BitwisePartitionStreamer next,
        boolean immutable
    ) {
        this.partition = Objects.requireNonNull(partition, "partition");
        this.spliterator = new BitwisePartitionSpliterator(
            partition,
            memorySegmentSource.get(partition),
            PartitionActionMediator.create(partition, shape),
            next == null ? null : next.spliterator,
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
