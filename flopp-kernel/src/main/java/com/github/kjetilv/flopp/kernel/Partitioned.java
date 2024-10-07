package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.formats.Format;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface Partitioned<P> extends Closeable {

    P partitioned();

    Partitions partitions();

    PartitionedProcessor<LineSegment, String> processor(Path target);

    PartitionedProcessor<SeparatedLine, Stream<LineSegment>> processor(Path target, Format format);

    Stream<CompletableFuture<PartitionResult<Void>>> forEachLine(
        BiConsumer<Partition, Stream<LineSegment>> consumer
    );

    <T> Stream<CompletableFuture<PartitionResult<T>>> map(
        BiFunction<Partition, Stream<LineSegment>, T> processor,
        ExecutorService executorService
    );

    Stream<LongSupplier> lineCounters();

    Stream<? extends PartitionStreamer> streamers();

    Stream<? extends CompletableFuture<PartitionStreamer>> streamers(ExecutorService executorService);

    Stream<PartitionedSplitter> splitters(Format format);

    Stream<CompletableFuture<PartitionedSplitter>> splitters(
        Format format,
        ExecutorService executorService
    );

    @Override
    void close();
}
