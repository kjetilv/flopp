package com.github.kjetilv.flopp.kernel;

import java.util.Objects;
import java.util.stream.Stream;

class DefaultPartitionedStreams implements PartitionedStreams {

    private final Shape shape;

    private final Partitioning partitioning;

    private final ByteSources sources;

    private final MemorySegmentSources memorySegmentSources;

    DefaultPartitionedStreams(
        Shape shape,
        Partitioning partitioning,
        ByteSources sources,
        MemorySegmentSources memorySegmentSources
    ) {
        this.shape = Objects.requireNonNull(shape, "shape");
        this.partitioning = Objects.requireNonNull(partitioning, "partitioning");
        this.sources = Objects.requireNonNull(sources, "sources");
        this.memorySegmentSources = Objects.requireNonNull(memorySegmentSources, "memorySegmentSources");
    }

    @Override
    public Stream<Streamer> streamers() {
        return Partition.partitions(shape.size(), partitioning.partitionCount())
            .stream()
            .filter(Partition::hasData)
            .map(partition ->
                new DefaultStreamer(
                    partition,
                    shape,
                    partitioning,
                    sources,
                    memorySegmentSources
                ));
    }

    @Override
    public void close() {
        sources.close();
    }
}
