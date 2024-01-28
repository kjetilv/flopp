package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.PartitionResult;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public class BitwisePartitionedMapper {

    private final BitwisePartitionStreamers streams;

    BitwisePartitionedMapper(BitwisePartitionStreamers streams) {
        this.streams = Objects.requireNonNull(streams, "streams");
    }

    public <T> Stream<PartitionResult<T>> map(
        BiFunction<Partition, Stream<LineSegment>, T> processor
    ) {
        return streams.streamers()
            .map(streamer ->
                new PartitionResult<>(
                    streamer.partition(),
                    processor.apply(streamer.partition(), streamer.lines())
                ));
    }

    public <T> Stream<CompletableFuture<PartitionResult<T>>> map(
        BiFunction<Partition, Stream<LineSegment>, T> processor,
        ExecutorService executorService
    ) {
        return streams.streamers()
            .map(streamer ->
                CompletableFuture.supplyAsync(
                        () ->
                            processor.apply(
                                streamer.partition(),
                                streamer.lines()
                            ),
                        executorService
                    )
                    .thenApply(result ->
                        new PartitionResult<>(streamer.partition(), result)));
    }
}
