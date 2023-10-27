package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface PartitionedConsumer extends Closeable {

    default void awaitForEach(BiConsumer<Partition, Stream<NpLine>> consumer) {
        Futures.await(forEach(consumer));
    }

    Stream<CompletableFuture<PartitionResult<Void>>> forEach(BiConsumer<Partition, Stream<NpLine>> consumer);

    @Override
    default void close() {
    }
}
