package com.github.kjetilv.flopp.kernel;

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

    PartitionedProcessor<LineSegment> processor(Path target);

    PartitionedMapper mapper();

    PartitionedConsumer consumer();

    PartitionedSplitters splitters();

    default Stream<PartitionedSplitter> splitters(CsvFormat format) {
        return splitters().splitters(format);
    }

    default Stream<CompletableFuture<PartitionedSplitter>> splitters(
        CsvFormat format,
        ExecutorService executorService
    ) {
        return splitters().splitters(format, executorService);
    }

    default Stream<PartitionedSplitter> splitters(FwFormat format) {
        return splitters().splitters(format);
    }

    default Stream<CompletableFuture<PartitionedSplitter>> splitters(FwFormat format, ExecutorService executorService) {
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
