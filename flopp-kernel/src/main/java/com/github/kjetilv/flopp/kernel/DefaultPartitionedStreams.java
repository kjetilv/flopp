package com.github.kjetilv.flopp.kernel;

import java.util.Objects;
import java.util.stream.Stream;

class DefaultPartitionedStreams implements PartitionedStreams {

    private final Shape shape;

    private final Partitioning partitioning;

    private final Sources sources;

    DefaultPartitionedStreams(
        Shape shape,
        Partitioning partitioning,
        Sources sources
    ) {
        this.shape = Objects.requireNonNull(shape, "shape");
        this.partitioning = Objects.requireNonNull(partitioning, "partitioning");
        this.sources = Objects.requireNonNull(sources, "sources");
    }

    @Override
    public Stream<VectorPartitionStreamer> vectorStreamers() {
        return Partition.partitions(shape.size(), partitioning.partitionCount())
            .stream()
            .filter(Partition::hasData)
            .map(partition ->
                new DefaultVectorPartitionStreamer(partition, shape, sources.memorySegmentSources()));
    }

    @Override
    public void close() {
        sources.close();
    }

    @Override
    public Stream<PartitionStreamer> streamers() {
        return Partition.partitions(shape.size(), partitioning.partitionCount())
            .stream()
            .filter(Partition::hasData)
            .map(partition ->
                new DefaultPartitionStreamer(
                    partition,
                    shape,
                    partitioning,
                    sources.byteSources()
                ));
    }
}
