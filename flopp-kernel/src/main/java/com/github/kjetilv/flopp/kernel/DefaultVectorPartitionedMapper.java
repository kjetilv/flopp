package com.github.kjetilv.flopp.kernel;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Function;
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
        return futureStream(processor, PartitionedStreams.VectorPartitionStreamer::memorySegments);
    }

    private <T, B> Stream<CompletableFuture<PartitionResult<T>>> futureStream(
        BiFunction<Partition, Stream<B>, T> processor,
        Function<PartitionedStreams.VectorPartitionStreamer, Stream<B>> segmentSuppliers
    ) {
        return streams.vectorStreamers()
            .map(streamer ->
                CompletableFuture.supplyAsync(
                        () ->
                            processor.apply(streamer.partition(), segmentSuppliers.apply(streamer)),
                        this.executorService
                    )
                    .thenApply(result ->
                        new PartitionResult<>(streamer.partition(), result)));
    }
}
