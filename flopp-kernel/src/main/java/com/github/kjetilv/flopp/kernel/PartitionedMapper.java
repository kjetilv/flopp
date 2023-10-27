package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface PartitionedMapper extends Closeable {

    default <T> List<PartitionResult<T>> awaitMap(BiFunction<Partition, Stream<NpLine>, T> processor) {
        return Futures.await(map(processor));
    }

    <T> Stream<CompletableFuture<PartitionResult<T>>> map(
        BiFunction<Partition, Stream<NpLine>, T> processor
    );

    @Override
    default void close() {
    }
}
