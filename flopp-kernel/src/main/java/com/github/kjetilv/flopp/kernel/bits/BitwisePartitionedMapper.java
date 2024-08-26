package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

final class BitwisePartitionedMapper implements PartitionedMapper {

    private final PartitionedStreams streams;

    BitwisePartitionedMapper(PartitionedStreams streams) {
        this.streams = Objects.requireNonNull(streams, "streams");
    }

    @Override
    public <T> Stream<CompletableFuture<PartitionResult<T>>> map(
        BiFunction<Partition, Stream<LineSegment>, T> processor,
        ExecutorService executorService
    ) {
        return streams.streamers()
            .map(streamer ->
                CompletableFuture.supplyAsync(lines(processor, streamer), executorService)
                    .thenApply(result ->
                        new PartitionResult<>(streamer.partition(), result)));
    }

    private static <T> Supplier<T> lines(
        BiFunction<Partition, Stream<LineSegment>, T> processor,
        PartitionStreamer streamer
    ) {
        return () -> processor.apply(streamer.partition(), streamer.lines());
    }
}
