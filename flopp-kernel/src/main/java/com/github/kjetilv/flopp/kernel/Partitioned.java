package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.formats.FlatFileFormat;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface Partitioned<P> extends Closeable {

    P partitioned();

    Partitions partitions();

    PartitionedProcessor<LineSegment, String> processor(Path target);

    <F extends FlatFileFormat<F>> PartitionedProcessor<SeparatedLine, Stream<LineSegment>> processor(
        Path target,
        F format
    );

    PartitionedMapper<LineSegment> mapper();

    PartitionedConsumer consumer();

    PartitionedSplitters splitters();

    default <F extends FlatFileFormat<F>> Stream<PartitionedSplitter> splitters(F format) {
        return splitters().splitters(format);
    }

    default <F extends FlatFileFormat<F>> Stream<CompletableFuture<PartitionedSplitter>> splitters(
        F format,
        ExecutorService executorService
    ) {
        return splitters().splitters(format, executorService);
    }

    PartitionedStreams streams();

    @Override
    void close();

    private static <T> List<T> awaitCompleted(Stream<CompletableFuture<T>> futures) {
        return futures
            .toList()
            .stream()
            .map(CompletableFuture::join)
            .toList();
    }
}
