package com.github.kjetilv.flopp.kernel;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface PartitionedMapper {

    <T> Stream<CompletableFuture<PartitionResult<T>>> map(
        BiFunction<Partition, Stream<LineSegment>, T> processor,
        ExecutorService executorService
    );
}
