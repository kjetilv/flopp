package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.formats.Format;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;

import java.io.Closeable;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface Partitioned<P> extends Closeable {

    P partitioned();

    Partitions partitions();

    Stream<PartitionResult<Void>> forEachLine(BiConsumer<Partition, Stream<LineSegment>> consumer);

    <T> Stream<PartitionResult<T>> map(BiFunction<Partition, Stream<LineSegment>, T> processor);

    Stream<LongSupplier> lineCounters();

    Stream<? extends PartitionStreamer> streamers();

    Stream<PartitionedSplitter> splitters(Format format);

    @Override
    void close();
}
