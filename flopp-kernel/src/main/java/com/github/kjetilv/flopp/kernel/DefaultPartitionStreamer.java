package com.github.kjetilv.flopp.kernel;

import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

final class DefaultPartitionStreamer implements PartitionedStreams.PartitionStreamer {

    private final ByteSources sources;

    private final Partition partition;

    private final int bufferSize;

    private final Shape shape;

    DefaultPartitionStreamer(
        Partition partition,
        Shape shape,
        Partitioning partitioning,
        ByteSources sources
    ) {
        this.partition = Objects.requireNonNull(partition, "partition");
        this.bufferSize = Objects.requireNonNull(partitioning, "partitioning")
            .bufferSizeOr(DEFAULT_BUFFER_SIZE);
        this.shape = Objects.requireNonNull(shape, "shape");
        this.sources = Objects.requireNonNull(sources, "sources");
    }

    @Override
    public Partition partition() {
        return partition;
    }

    @Override
    public Stream<String> lines() {
        ByteSource source = sources.source(partition);
        return StreamSupport.stream(
            shape.limitsLineLength()
                ? new StringLimitedPartitionSpliterator(source, partition, shape, bufferSize)
                : new StringGrowingPartitionSpliterator(source, partition, shape, bufferSize),
            false
        );
    }

    @Override
    public Stream<byte[]> rawLines() {
        ByteSource source = sources.source(partition);
        return StreamSupport.stream(
            shape.limitsLineLength()
                ? new BytesLimitedPartitionSpliterator(source, partition, shape, bufferSize)
                : new BytesGrowingPartitionSpliterator(source, partition, shape, bufferSize),
            false
        );
    }

    @Override
    public Stream<NLine> nLines() {
        ByteSource source = sources.source(partition);
        return StreamSupport.stream(
            shape.limitsLineLength()
                ? new NLineLimitedPartitionSpliterator(source, partition, shape, bufferSize)
                : new NLineGrowingPartitionSpliterator(source, partition, shape, bufferSize),
            false
        );
    }

    @Override
    public Stream<RNLine> rnLines() {
        ByteSource source = sources.source(partition);
        return StreamSupport.stream(
            shape.limitsLineLength()
                ? new RNLineLimitedPartitionSpliterator(source, partition, shape, bufferSize)
                : new RNLineGrowingPartitionSpliterator(source, partition, shape, bufferSize),
            false
        );
    }

    @Override
    public Stream<ByteSeg> byteSegs() {
        ByteSource source = sources.source(partition);
        return StreamSupport.stream(
            shape.limitsLineLength()
                ? new ByteSegLimitedPartitionSpliterator(source, partition, shape, bufferSize)
                : new ByteSegGrowingPartitionSpliterator(source, partition, shape, bufferSize),
            false
        );
    }

    @Override
    public Stream<Supplier<ByteSeg>> suppliedByteSegs() {
        ByteSource source = sources.source(partition);
        return StreamSupport.stream(
            new ByteSegSupGrowingPartitionSpliterator(source, partition, shape, bufferSize),
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

    private static final int DEFAULT_BUFFER_SIZE = 8192;
}
