package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

@SuppressWarnings("unused")
@FunctionalInterface
public interface PartitionedConsumer extends Closeable {

    default void awaitForEach(BiConsumer<Partition, Stream<NLine>> consumer) {
        Futures.awaitCompleted(forEach(consumer));
    }

    Stream<CompletableFuture<PartitionResult<Void>>> forEach(BiConsumer<Partition, Stream<NLine>> consumer);

    @Override
    default void close() {
    }
}
