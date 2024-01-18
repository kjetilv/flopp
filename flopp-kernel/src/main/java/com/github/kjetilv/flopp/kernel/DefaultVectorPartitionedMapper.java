package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.MemorySegments;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.stream.Stream;

class DefaultVectorPartitionedMapper implements VectorPartitionedMapper {

    private final ExecutorService executorService;

    private final PartitionedStreams streams;

    DefaultVectorPartitionedMapper(PartitionedStreams streams, ExecutorService executorService) {
        this.streams = Objects.requireNonNull(streams, "streams");
        this.executorService = Objects.requireNonNull(executorService, "executorService");
    }

    @Override
    public <T> Stream<CompletableFuture<PartitionResult<T>>> mapLines(
        BiFunction<Partition, Stream<MemorySegments.LineSegment>, T> processor
    ) {
        return streams.vectorStreamers()
            .map(streamer ->
                CompletableFuture.supplyAsync(
                        () ->
                            processor.apply(streamer.partition(), streamer.memorySegments()),
                        this.executorService
                    )
                    .thenApply(result ->
                        new PartitionResult<>(streamer.partition(), result)));
    }

}
