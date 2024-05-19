package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

final class BitwisePartitionedSplitters implements PartitionedSplitters {

    private final PartitionedStreams streams;

    BitwisePartitionedSplitters(PartitionedStreams partitionedStreams) {
        this.streams = Objects.requireNonNull(partitionedStreams, "partitionedStreams");
    }

    @Override
    public List<PartitionedSplitter> splittersList(CsvFormat format, boolean immutable) {
        return splitters(format, immutable).toList();
    }

    @Override
    public Stream<PartitionedSplitter> splitters(FwFormat format, boolean immutable) {
        return streams.streamers(immutable)
            .map(streamer ->
                new BitwiseFwSplitter(streamer, format, immutable));
    }

    @Override
    public Stream<PartitionedSplitter> splitters(CsvFormat format, boolean immutable) {
        return streams.streamers(immutable)
            .map(streamer ->
                new BitwiseCsvSplitter(streamer, format, immutable));
    }

    @Override
    public List<PartitionedSplitter> splittersList(FwFormat format, boolean immutable) {
        return splitters(format, immutable).toList();
    }
}
