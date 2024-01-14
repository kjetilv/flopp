package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface VectorPartitionedMapper extends Closeable {

    <T> Stream<CompletableFuture<PartitionResult<T>>> mapLines(
        BiFunction<Partition, Stream<MemorySegments.LineSegment>, T> processor
    );

    @Override
    default void close() {
    }
}
