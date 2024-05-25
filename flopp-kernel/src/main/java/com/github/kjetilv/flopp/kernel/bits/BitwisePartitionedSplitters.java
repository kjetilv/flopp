package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;

import java.util.Objects;
import java.util.stream.Stream;

final class BitwisePartitionedSplitters implements PartitionedSplitters {

    private final PartitionedStreams streams;

    private final Shape shape;

    BitwisePartitionedSplitters(PartitionedStreams partitionedStreams, Shape shape) {
        this.streams = Objects.requireNonNull(partitionedStreams, "partitionedStreams");
        this.shape = Objects.requireNonNull(shape, "shape");
    }

    @Override
    public Stream<PartitionedSplitter> splitters(FwFormat format) {
        return streams.streamers()
            .map(streamer ->
                new BitwiseFwSplitter(streamer, format.withCharset(shape.charset())));
    }

    @Override
    public Stream<PartitionedSplitter> splitters(CsvFormat format) {
        return streams.streamers()
            .map(streamer ->
                new BitwiseCsvSplitter(streamer, format.withCharset(shape.charset())));
    }
}
