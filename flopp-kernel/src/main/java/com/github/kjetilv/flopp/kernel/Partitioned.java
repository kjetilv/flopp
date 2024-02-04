package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.LineSegment;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface Partitioned<P> extends Closeable {

    P partitioned();

    List<Partition> partitions();

    PartitionedStreams streams();

    PartitionedMapper mapper();

    PartitionedConsumer consumer();

    PartitionedProcessor<LineSegment> processor(Path target);

    @Override
    default void close() {
    }

    private static <T> List<T> awaitCompleted(Stream<CompletableFuture<T>> futures) {
        return futures
            .toList()
            .stream()
            .map(CompletableFuture::join)
            .toList();
    }
}
