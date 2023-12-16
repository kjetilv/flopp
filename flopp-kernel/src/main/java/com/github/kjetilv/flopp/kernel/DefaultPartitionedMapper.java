package com.github.kjetilv.flopp.kernel;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

class DefaultPartitionedMapper implements PartitionedMapper {

    private final ExecutorService executorService;

    private final ByteSources sources;

    private final PartitionedStreams streams;

    DefaultPartitionedMapper(
        PartitionedStreams streams,
        ByteSources sources,
        ExecutorService executorService
    ) {
        this.streams = Objects.requireNonNull(streams, "streams");
        this.executorService = Objects.requireNonNull(executorService, "executorService");
        this.sources = Objects.requireNonNull(sources, "sources");
    }

    @Override
    public <T> Stream<CompletableFuture<PartitionResult<T>>> map(BiFunction<Partition, Stream<NpLine>, T> processor) {
        return streams.streamers()
            .map(streamer ->
                CompletableFuture.supplyAsync(streamer(processor, streamer), this.executorService)
                    .thenApply(result ->
                        new PartitionResult<>(streamer.partition(), result)));
    }

    @Override
    public void close() {
        sources.close();
    }

    private static <T> Supplier<T> streamer(
        BiFunction<Partition, Stream<NpLine>, T> processor,
        PartitionedStreams.Streamer streamer
    ) {
        return () ->
            processor.apply(streamer.partition(), streamer.lines());
    }
}
