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
    public List<PartitionedSplitter> splittersList(CsvFormat csvFormat, boolean immutable) {
        return splitters(csvFormat, immutable).toList();
    }

    @Override
    public Stream<PartitionedSplitter> splitters(CsvFormat csvFormat, boolean immutable) {
        return streams.streamers(immutable)
            .map(streamer ->
                new BitwiseCsvSplitter(streamer, csvFormat));
    }

    @Override
    public Stream<PartitionedSplitter> splitters(FwFormat fwFormat, boolean immutable) {
        return streams.streamers(immutable)
            .map(streamer ->
                new BitwiseFwSplitter(streamer, fwFormat));
    }

    @Override
    public List<PartitionedSplitter> splittersList(FwFormat fwFormat, boolean immutable) {
        return splitters(fwFormat, immutable).toList();
    }
}
