package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface Partitioned extends Closeable {

    Partitions partitions();

    Stream<LongSupplier> lineCounters();

    Stream<PartitionStreamer> streamers();

    Stream<PartitionedSplitter> splitters(Format format);

    @Override
    void close();
}
