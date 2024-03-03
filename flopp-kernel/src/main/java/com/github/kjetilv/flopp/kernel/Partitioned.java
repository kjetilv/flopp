package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface Partitioned<P> extends Closeable {

    P partitioned();

    List<Partition> partitions();

    PartitionedProcessor<LineSegment> processor(Path target);

    PartitionedMapper mapper();

    PartitionedConsumer consumer();

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
