package com.github.kjetilv.flopp.kernel;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
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
    public <T> Stream<CompletableFuture<PartitionResult<T>>> mapNLines(
        BiFunction<Partition, Stream<NLine>, T> processor
    ) {
        return streams.streamers()
            .map(streamer ->
                CompletableFuture.supplyAsync(
                    () ->
                        processor.apply(streamer.partition(), streamer.nLines()),
                    this.executorService
                ).thenApply(result ->
                    new PartitionResult<>(streamer.partition(), result)));
    }

    @Override
    public <T> Stream<CompletableFuture<PartitionResult<T>>> mapRNLines(
        BiFunction<Partition, Stream<RNLine>, T> processor
    ) {
        return streams.streamers()
            .map(streamer ->
                CompletableFuture.supplyAsync(
                    () ->
                        processor.apply(streamer.partition(), streamer.rnLines()),
                    this.executorService
                ).thenApply(result ->
                    new PartitionResult<>(streamer.partition(), result)));
    }

    @Override
    public <T> Stream<CompletableFuture<PartitionResult<T>>> mapLines(
        BiFunction<Partition, Stream<String>, T> processor
    ) {
        return streams.streamers()
            .map(streamer ->
                CompletableFuture.supplyAsync(
                    () ->
                        processor.apply(streamer.partition(), streamer.lines()),
                    this.executorService
                ).thenApply(result ->
                    new PartitionResult<>(streamer.partition(), result)));
    }

    @Override
    public <T> Stream<CompletableFuture<PartitionResult<T>>> mapRawLines(
        BiFunction<Partition, Stream<byte[]>, T> processor
    ) {
        return streams.streamers()
            .map(streamer ->
                CompletableFuture.supplyAsync(
                    () ->
                        processor.apply(streamer.partition(), streamer.rawLines()),
                    this.executorService
                ).thenApply(result ->
                    new PartitionResult<>(streamer.partition(), result)));
    }

    @Override
    public <T> Stream<CompletableFuture<PartitionResult<T>>> mapSegments(
        BiFunction<Partition, Stream<ByteSeg>, T> processor
    ) {
        return streams.streamers()
            .map(streamer ->
                CompletableFuture.supplyAsync(
                    () ->
                        processor.apply(streamer.partition(), streamer.segments()),
                    this.executorService
                ).thenApply(result ->
                    new PartitionResult<>(streamer.partition(), result)));
    }

    @Override
    public void close() {
        sources.close();
    }
}
