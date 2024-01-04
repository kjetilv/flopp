package com.github.kjetilv.flopp.kernel;

import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

final class DefaultStreamer implements PartitionedStreams.Streamer {

    private final ByteSource source;

    private final Partition partition;

    private final int bufferSize;

    private final Shape shape;

    DefaultStreamer(ByteSource source, Partition partition, Shape shape, int bufferSize) {
        this.source = Objects.requireNonNull(source, "randomAccessFile");
        this.partition = Objects.requireNonNull(partition, "partition");
        this.bufferSize = bufferSize > 0 ? bufferSize : DEFAULT_SLICE_SIZE;
        this.shape = Objects.requireNonNull(shape, "shape");
    }

    @Override
    public Partition partition() {
        return partition;
    }

    @Override
    public Stream<String> lines() {
        return StreamSupport.stream(
            new RawStringPartitionSpliterator(source, partition, shape, bufferSize),
            false);
    }

    @Override
    public Stream<NLine> nLines() {
        return StreamSupport.stream(
            new NLinePartitionSpliterator(source, partition, shape, bufferSize),
            false);
    }

    @Override
    public Stream<RNLine> rawNLines() {
        return StreamSupport.stream(
            new RNLinePartitionSpliterator(source, partition, shape, bufferSize),
            false);
    }

    @Override
    public Stream<byte[]> rawByteLines() {
        return StreamSupport.stream(
            new RawBytesPartitionSpliterator(source, partition, shape, bufferSize),
            false);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + partition + "]";
    }

    private static final int DEFAULT_SLICE_SIZE = 8192;
}
