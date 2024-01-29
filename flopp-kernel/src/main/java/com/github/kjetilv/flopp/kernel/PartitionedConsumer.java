package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface PartitionedConsumer extends Closeable {

    Stream<CompletableFuture<PartitionResult<Void>>> forEachLine(
        BiConsumer<Partition, Stream<LineSegment>> consumer
    );

    @Override
    default void close() {
    }
}
