package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;

import java.util.Objects;
import java.util.stream.Stream;

final class BitwisePartitionedSplitters implements PartitionedSplitters {

    private final PartitionedStreams streams;

    BitwisePartitionedSplitters(PartitionedStreams partitionedStreams) {
        this.streams = Objects.requireNonNull(partitionedStreams, "partitionedStreams");
    }

    @Override
    public Stream<PartitionedSplitter> splitters(FwFormat format) {
        return streams.streamers()
            .map(streamer ->
                new BitwiseFwSplitter(streamer, format));
    }

    @Override
    public Stream<PartitionedSplitter> splitters(CsvFormat format) {
        return streams.streamers()
            .map(streamer ->
                new BitwiseCsvSplitter(streamer, format));
    }
}
