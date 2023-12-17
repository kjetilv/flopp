package com.github.kjetilv.flopp.kernel;

import java.util.Objects;
import java.util.stream.Stream;

class DefaultPartitionedStreams implements PartitionedStreams{

    private final Shape shape;

    private final Partitioning partitioning;

    private final ByteSources sources;

    DefaultPartitionedStreams(
        Shape shape,
        Partitioning partitioning,
        ByteSources sources
    ) {
        this.shape = Objects.requireNonNull(shape, "shape");
        this.partitioning = Objects.requireNonNull(partitioning, "partitioning");
        this.sources = Objects.requireNonNull(sources, "sources");
    }

    @Override
    public Stream<Streamer> streamers() {
        return Partition.partitions(shape.size(), partitioning.partitionCount())
            .stream()
            .filter(Partition::hasData)
            .map(this::streamer);
    }

    @Override
    public void close() {
        sources.close();
    }

    private Streamer streamer(Partition partition) {
        return new DefaultStreamer(
            sources.source(partition),
            partition,
            shape,
            partitioning.bufferSize()
        );
    }
}
