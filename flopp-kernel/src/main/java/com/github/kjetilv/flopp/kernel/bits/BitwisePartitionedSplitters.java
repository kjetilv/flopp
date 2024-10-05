package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.PartitionedSplitter;
import com.github.kjetilv.flopp.kernel.PartitionedSplitters;
import com.github.kjetilv.flopp.kernel.PartitionedStreams;
import com.github.kjetilv.flopp.kernel.Shape;
import com.github.kjetilv.flopp.kernel.formats.CsvFormat;
import com.github.kjetilv.flopp.kernel.formats.FlatFileFormat;
import com.github.kjetilv.flopp.kernel.formats.FwFormat;

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
    public Stream<PartitionedSplitter> splitters(FlatFileFormat format) {
        return switch (format) {
            case CsvFormat csvFormat -> {
                CsvFormat csv = csvFormat.withCharset(shape.charset());
                yield streams.streamers()
                    .map(streamer ->
                        new BitwiseCsvSplitter(streamer, csv));
            }
            case FwFormat fwFormat -> {
                FwFormat fw = fwFormat.withCharset(shape.charset());
                yield streams.streamers()
                    .map(streamer ->
                        new BitwiseFwSplitter(streamer, fw));
            }
        };
    }

    @Override
    public Stream<CompletableFuture<PartitionedSplitter>> splitters(
        FlatFileFormat format,
        ExecutorService executorService
    ) {
        return switch (format) {
            case CsvFormat csvFormat -> {
                CsvFormat csv = csvFormat.withCharset(shape.charset());
                yield streams.streamers(executorService)
                    .map(future ->
                        future.thenApply(streamer ->
                            new BitwiseCsvSplitter(streamer, csv)));
            }
            case FwFormat fwFormat -> {
                FwFormat fw = fwFormat.withCharset(shape.charset());
                yield streams.streamers(executorService)
                    .map(future ->
                        future.thenApply(streamer ->
                            new BitwiseFwSplitter(streamer, fw)));
            }
        };
    }

}
