package com.github.kjetilv.flopp.kernel;

import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

final class DefaultStreamer implements PartitionedStreams.Streamer {

    private final ByteSources byteSources;

    private final Partition partition;

    private final int bufferSize;

    private final Shape shape;

    private final MemorySegmentSources memorySegmentSources;

    DefaultStreamer(
        Partition partition,
        Shape shape,
        Partitioning partitioning,
        ByteSources byteSources,
        MemorySegmentSources memorySegmentSources
    ) {
        this.partition = Objects.requireNonNull(partition, "partition");
        this.bufferSize = Objects.requireNonNull(partitioning, "partitioning")
            .bufferSizeOr(DEFAULT_BUFFER_SIZE);
        this.shape = Objects.requireNonNull(shape, "shape");
        this.byteSources = Objects.requireNonNull(byteSources, "sources");
        this.memorySegmentSources = Objects.requireNonNull(memorySegmentSources, "memorySegmentSources");
    }

    @Override
    public Partition partition() {
        return partition;
    }

    @Override
    public Stream<String> lines() {
        return StreamSupport.stream(
            shape.limitsLineLength()
                ? new StringLimitedPartitionSpliterator(source(), partition, shape, bufferSize)
                : new StringGrowingPartitionSpliterator(source(), partition, shape, bufferSize),
            false
        );
    }

    @Override
    public Stream<byte[]> rawLines() {
        return StreamSupport.stream(
            shape.limitsLineLength()
                ? new BytesLimitedPartitionSpliterator(source(), partition, shape, bufferSize)
                : new BytesGrowingPartitionSpliterator(source(), partition, shape, bufferSize),
            false
        );
    }

    @Override
    public Stream<NLine> nLines() {
        return StreamSupport.stream(
            shape.limitsLineLength()
                ? new NLineLimitedPartitionSpliterator(source(), partition, shape, bufferSize)
                : new NLineGrowingPartitionSpliterator(source(), partition, shape, bufferSize),
            false
        );
    }

    @Override
    public Stream<RNLine> rnLines() {
        return StreamSupport.stream(
            shape.limitsLineLength()
                ? new RNLineLimitedPartitionSpliterator(source(), partition, shape, bufferSize)
                : new RNLineGrowingPartitionSpliterator(source(), partition, shape, bufferSize),
            false
        );
    }

    @Override
    public Stream<ByteSeg> segments() {
        return StreamSupport.stream(
            shape.limitsLineLength()
                ? new ByteSegLimitedPartitionSpliterator(source(), partition, shape, bufferSize)
                : new ByteSegGrowingPartitionSpliterator(source(), partition, shape, bufferSize),
            false
        );
    }

    @Override
    public Stream<Supplier<ByteSeg>> segmentSuppliers() {
        return StreamSupport.stream(
            new ByteSegSupGrowingPartitionSpliterator(source(), partition, shape, bufferSize),
            false
        );
    }

    @Override
    public Stream<MemorySegments.Line> memorySegments() {
        return StreamSupport.stream(
            new MemorySegmentPartitionSpliterator(partition, memorySegmentSources),
            false
        );
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{partition}]";
    }

    private ByteSource source() {
        return byteSources.source(partition);
    }

    private static final int DEFAULT_BUFFER_SIZE = 8192;
}
