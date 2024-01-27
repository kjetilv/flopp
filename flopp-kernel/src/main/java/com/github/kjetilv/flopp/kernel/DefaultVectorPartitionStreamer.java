package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.LineSegment;
import com.github.kjetilv.flopp.kernel.bits.LineSegments;

import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

final class DefaultVectorPartitionStreamer implements PartitionedStreams.VectorPartitionStreamer {

    private final Shape shape;

    private final MemorySegmentSources sources;

    private final Partition partition;

    DefaultVectorPartitionStreamer(Partition partition, Shape shape, MemorySegmentSources sources) {
        this.partition = Objects.requireNonNull(partition, "partition");
        this.shape = Objects.requireNonNull(shape, "shape");
        this.sources = Objects.requireNonNull(sources, "sources");
    }

    @Override
    public Partition partition() {
        return partition;
    }

    @Override
    public Stream<LineSegment> memorySegments() {
        MemorySegmentSource source = sources.source(partition);
        return StreamSupport.stream(
            new VectorPartitionSpliterator(
                partition,
                source,
                LineSegments.mediator(partition, shape)
            ),
            false
        );
    }

    @Override
    public void close() {
        sources.close();
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{partition}]";
    }
}
