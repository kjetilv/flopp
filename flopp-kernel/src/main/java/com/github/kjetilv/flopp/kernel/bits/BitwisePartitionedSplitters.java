package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.PartitionedSplitter;
import com.github.kjetilv.flopp.kernel.PartitionedSplitters;
import com.github.kjetilv.flopp.kernel.PartitionedStreams;
import com.github.kjetilv.flopp.kernel.formats.Format;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

final class BitwisePartitionedSplitters implements PartitionedSplitters {

    private final PartitionedStreams streams;

    BitwisePartitionedSplitters(PartitionedStreams partitionedStreams) {
        this.streams = Objects.requireNonNull(partitionedStreams, "partitionedStreams");
    }

    @Override
    public Stream<PartitionedSplitter> splitters(Format format) {
        return switch (format) {
            case Format.Csv csv -> streams.streamers()
                .map(streamer ->
                    new BitwiseCsvSplitter(streamer, csv));
            case Format.FwFormat fw -> streams.streamers()
                .map(streamer ->
                    new BitwiseFwSplitter(streamer, fw));
        };
    }

    @Override
    public Stream<CompletableFuture<PartitionedSplitter>> splitters(
        Format format,
        ExecutorService executorService
    ) {
        return switch (format) {
            case Format.Csv csv -> streams.streamers(executorService)
                .map(future ->
                    future.thenApply(streamer ->
                        new BitwiseCsvSplitter(streamer, csv)));
            case Format.FwFormat fw -> streams.streamers(executorService)
                .map(future ->
                    future.thenApply(streamer ->
                        new BitwiseFwSplitter(streamer, fw)));
        };
    }
}
