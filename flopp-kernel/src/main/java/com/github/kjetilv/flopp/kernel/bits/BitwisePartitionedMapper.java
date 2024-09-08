package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.PartitionResult;
import com.github.kjetilv.flopp.kernel.PartitionedMapper;
import com.github.kjetilv.flopp.kernel.PartitionedStreams;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.stream.Stream;

final class BitwisePartitionedMapper implements PartitionedMapper<LineSegment> {

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
                CompletableFuture.supplyAsync(
                        () ->
                            processor.apply(streamer.partition(), streamer.lines()),
                        executorService
                    )
                    .thenApply(result ->
                        new PartitionResult<>(streamer.partition(), result)));
    }

}
