package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;
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
        return StreamSupport.stream(
            shape.limitsLineLength()
                ? new StringLimitedPartitionSpliterator(sources.source(partition), partition, shape, bufferSize)
                : new StringGrowingPartitionSpliterator(sources.source(partition), partition, shape, bufferSize),
            false
        );
    }

    @Override
    public Stream<byte[]> rawLines() {
        return StreamSupport.stream(
            shape.limitsLineLength()
                ? new BytesLimitedPartitionSpliterator(sources.source(partition), partition, shape, bufferSize)
                : new BytesGrowingPartitionSpliterator(sources.source(partition), partition, shape, bufferSize),
            false
        );
    }

    @Override
    public Stream<NLine> nLines() {
        return StreamSupport.stream(
            shape.limitsLineLength()
                ? new NLineLimitedPartitionSpliterator(sources.source(partition), partition, shape, bufferSize)
                : new NLineGrowingPartitionSpliterator(sources.source(partition), partition, shape, bufferSize),
            false
        );
    }

    @Override
    public Stream<RNLine> rnLines() {
        return StreamSupport.stream(
            shape.limitsLineLength()
                ? new RNLineLimitedPartitionSpliterator(sources.source(partition), partition, shape, bufferSize)
                : new RNLineGrowingPartitionSpliterator(sources.source(partition), partition, shape, bufferSize),
            false
        );
    }

    @Override
    public Stream<ByteSeg> byteSegs() {
        return StreamSupport.stream(
            shape.limitsLineLength()
                ? new ByteSegLimitedPartitionSpliterator(sources.source(partition), partition, shape, bufferSize)
                : new ByteSegGrowingPartitionSpliterator(sources.source(partition), partition, shape, bufferSize),
            false
        );
    }

    @Override
    public Stream<Supplier<ByteSeg>> suppliedByteSegs() {
        return StreamSupport.stream(
            new ByteSegSupGrowingPartitionSpliterator(sources.source(partition), partition, shape, bufferSize),
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
