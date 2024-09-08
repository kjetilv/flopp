package com.github.kjetilv.flopp.kernel;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface PartitionedMapper<I> {

    <T> Stream<CompletableFuture<PartitionResult<T>>> map(
        BiFunction<Partition, Stream<I>, T> processor,
        ExecutorService executorService
    );
}
