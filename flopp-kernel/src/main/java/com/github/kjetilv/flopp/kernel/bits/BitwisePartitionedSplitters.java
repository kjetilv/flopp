package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.PartitionedSplitter;
import com.github.kjetilv.flopp.kernel.PartitionedSplitters;
import com.github.kjetilv.flopp.kernel.PartitionedStreams;
import com.github.kjetilv.flopp.kernel.formats.CsvFormat;
import com.github.kjetilv.flopp.kernel.formats.FwFormat;
import com.github.kjetilv.flopp.kernel.Shape;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

final class BitwisePartitionedSplitters implements PartitionedSplitters {

    private final PartitionedStreams streams;

    private final Shape shape;

    BitwisePartitionedSplitters(PartitionedStreams partitionedStreams, Shape shape) {
        this.streams = Objects.requireNonNull(partitionedStreams, "partitionedStreams");
        this.shape = Objects.requireNonNull(shape, "shape");
    }

    @Override
    public Stream<PartitionedSplitter> splitters(CsvFormat format) {
        CsvFormat csvFormat = format.withCharset(shape.charset());
        return streams.streamers()
            .map(streamer ->
                new BitwiseCsvSplitter(streamer, csvFormat));
    }

    @Override
    public Stream<CompletableFuture<PartitionedSplitter>> splitters(CsvFormat format, ExecutorService executorService) {
        CsvFormat csvFormat = format.withCharset(shape.charset());
        return streams.streamers(executorService)
            .map(future ->
                future.thenApply(streamer ->
                    new BitwiseCsvSplitter(streamer, csvFormat)));
    }

    @Override
    public Stream<PartitionedSplitter> splitters(FwFormat format) {
        FwFormat fwFormat = format.withCharset(shape.charset());
        return streams.streamers()
            .map(streamer ->
                new BitwiseFwSplitter(streamer, fwFormat));
    }

    @Override
    public Stream<CompletableFuture<PartitionedSplitter>> splitters(FwFormat format, ExecutorService executorService) {
        FwFormat fwFormat = format.withCharset(shape.charset());
        return streams.streamers(executorService)
            .map(future ->
                future.thenApply(streamer ->
                    new BitwiseFwSplitter(streamer, fwFormat)));
    }
}
