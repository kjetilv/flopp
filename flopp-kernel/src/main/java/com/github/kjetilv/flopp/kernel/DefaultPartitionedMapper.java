package com.github.kjetilv.flopp.kernel;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

class DefaultPartitionedMapper implements PartitionedMapper {

    private final ExecutorService executorService;

    private final ByteSources sources;

    private final PartitionedStreams streams;

    DefaultPartitionedMapper(PartitionedStreams streams, ByteSources sources, ExecutorService executorService) {
        this.streams = Objects.requireNonNull(streams, "streams");
        this.executorService = Objects.requireNonNull(executorService, "executorService");
        this.sources = Objects.requireNonNull(sources, "sources");
    }

    @Override
    public <T> Stream<CompletableFuture<PartitionResult<T>>> mapLines(
        BiFunction<Partition, Stream<String>, T> processor
    ) {
        return futureStream(processor, PartitionedStreams.Streamer::lines);
    }

    @Override
    public <T> Stream<CompletableFuture<PartitionResult<T>>> mapRawLines(
        BiFunction<Partition, Stream<byte[]>, T> processor
    ) {
        return futureStream(processor, PartitionedStreams.Streamer::rawLines);
    }

    @Override
    public <T> Stream<CompletableFuture<PartitionResult<T>>> mapNLines(
        BiFunction<Partition, Stream<NLine>, T> processor
    ) {
        return futureStream(processor, PartitionedStreams.Streamer::nLines);
    }

    @Override
    public <T> Stream<CompletableFuture<PartitionResult<T>>> mapRNLines(
        BiFunction<Partition, Stream<RNLine>, T> processor
    ) {
        return futureStream(processor, PartitionedStreams.Streamer::rnLines);
    }

    @Override
    public <T> Stream<CompletableFuture<PartitionResult<T>>> mapSegments(
        BiFunction<Partition, Stream<ByteSeg>, T> processor
    ) {
        return futureStream(processor, PartitionedStreams.Streamer::segments);
    }

    @Override
    public <T> Stream<CompletableFuture<PartitionResult<T>>> mapSuppliedSegments(
        BiFunction<Partition, Stream<Supplier<ByteSeg>>, T> processor
    ) {
        return futureStream(processor, PartitionedStreams.Streamer::segmentSuppliers);
    }

    @Override
    public void close() {
        sources.close();
    }

    private <T, B> Stream<CompletableFuture<PartitionResult<T>>> futureStream(
        BiFunction<Partition, Stream<B>, T> processor,
        Function<PartitionedStreams.Streamer, Stream<B>> segmentSuppliers
    ) {
        return streams.streamers()
            .map(streamer ->
                CompletableFuture
                    .supplyAsync(
                        () ->
                            processor.apply(streamer.partition(), segmentSuppliers.apply(streamer)),
                        this.executorService
                    )
                    .thenApply(result ->
                        new PartitionResult<>(streamer.partition(), result)));
    }
}
