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

    private final PartitionedStreams streams;

    DefaultPartitionedMapper(PartitionedStreams streams, ExecutorService executorService) {
        this.streams = Objects.requireNonNull(streams, "streams");
        this.executorService = Objects.requireNonNull(executorService, "executorService");
    }

    @Override
    public <T> Stream<CompletableFuture<PartitionResult<T>>> mapLines(
        BiFunction<Partition, Stream<String>, T> processor
    ) {
        return futureStream(processor, PartitionedStreams.PartitionStreamer::lines);
    }

    @Override
    public <T> Stream<CompletableFuture<PartitionResult<T>>> mapRawLines(
        BiFunction<Partition, Stream<byte[]>, T> processor
    ) {
        return futureStream(processor, PartitionedStreams.PartitionStreamer::rawLines);
    }

    @Override
    public <T> Stream<CompletableFuture<PartitionResult<T>>> mapNLines(
        BiFunction<Partition, Stream<NLine>, T> processor
    ) {
        return futureStream(processor, PartitionedStreams.PartitionStreamer::nLines);
    }

    @Override
    public <T> Stream<CompletableFuture<PartitionResult<T>>> mapRNLines(
        BiFunction<Partition, Stream<RNLine>, T> processor
    ) {
        return futureStream(processor, PartitionedStreams.PartitionStreamer::rnLines);
    }

    @Override
    public <T> Stream<CompletableFuture<PartitionResult<T>>> mapByteSegs(
        BiFunction<Partition, Stream<ByteSeg>, T> processor
    ) {
        return futureStream(processor, PartitionedStreams.PartitionStreamer::byteSegs);
    }

    @Override
    public <T> Stream<CompletableFuture<PartitionResult<T>>> mapSuppliedByteSegs(
        BiFunction<Partition, Stream<Supplier<ByteSeg>>, T> processor
    ) {
        return futureStream(processor, PartitionedStreams.PartitionStreamer::suppliedByteSegs);
    }

    private <T, B> Stream<CompletableFuture<PartitionResult<T>>> futureStream(
        BiFunction<Partition, Stream<B>, T> processor,
        Function<PartitionedStreams.PartitionStreamer, Stream<B>> segmentSuppliers
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
