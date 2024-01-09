package com.github.kjetilv.flopp.kernel;

import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

final class DefaultStreamer implements PartitionedStreams.Streamer {

    private final ByteSource source;

    private final Partition partition;

    private final Partitioning partitioning;

    private final int bufferSize;

    private final Shape shape;

    DefaultStreamer(ByteSource source, Partition partition, Shape shape, Partitioning partitioning) {
        this.source = Objects.requireNonNull(source, "randomAccessFile");
        this.partition = Objects.requireNonNull(partition, "partition");
        this.partitioning = Objects.requireNonNull(partitioning, "partitioning");
        this.bufferSize = this.partitioning.bufferSizeOr(DEFAULT_BUFFER_SIZE);
        this.shape = Objects.requireNonNull(shape, "shape");
    }

    @Override
    public Partition partition() {
        return partition;
    }

    @Override
    public Stream<String> lines() {
        return StreamSupport.stream(
            new StringPartitionSpliterator(source, partition, shape, bufferSize),
            false);
    }

    @Override
    public Stream<byte[]> rawLines() {
        return StreamSupport.stream(
            new BytesPartitionSpliterator(source, partition, shape, bufferSize),
            false);
    }

    @Override
    public Stream<NLine> nLines() {
        return StreamSupport.stream(
            new NLinePartitionSpliterator(source, partition, shape, bufferSize),
            false);
    }

    @Override
    public Stream<RNLine> rnLines() {
        return StreamSupport.stream(
            new RNLinePartitionSpliterator(source, partition, shape, bufferSize),
            false);
    }

    @Override
    public Stream<ByteSeg> segments() {
        return StreamSupport.stream(
            new ByteSegPartitionSpliterator(source, partition, shape, bufferSize),
            false);
    }

    @Override
    public Stream<Supplier<ByteSeg>> segmentSuppliers() {
        return StreamSupport.stream(
            new ByteSegSupPartitionSpliterator(source, partition, shape, bufferSize),
            false);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + partition + "]";
    }

    private static final int DEFAULT_BUFFER_SIZE = 8192;
}
