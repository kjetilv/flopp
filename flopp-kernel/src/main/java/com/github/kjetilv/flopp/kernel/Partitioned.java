package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface Partitioned<P> extends Closeable {

    P partitioned();

    Partitions partitions();

    Stream<LongSupplier> lineCounters();

    Stream<PartitionStreamer> streamers();

    Stream<PartitionedSplitter> splitters(Format format);

    PartitionedProcessor<P, LineSegment, String> processTo(P target, Charset charSet);

    PartitionedProcessor<P, SeparatedLine, Stream<LineSegment>> processTo(P target, Format format);

    @Override
    void close();
}
