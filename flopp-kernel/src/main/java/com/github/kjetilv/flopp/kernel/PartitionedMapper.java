package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface PartitionedMapper extends Closeable {

    <T> Stream<CompletableFuture<PartitionResult<T>>> map(
        BiFunction<Partition, Stream<LineSegment>, T> processor, ExecutorService executorService
    );
    @Override
    default void close() {
    }
}
