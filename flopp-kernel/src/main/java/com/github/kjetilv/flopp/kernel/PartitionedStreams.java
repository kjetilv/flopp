package com.github.kjetilv.flopp.kernel;

import java.util.List;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface PartitionedStreams {

    default Stream<? extends PartitionStreamer> streamers() {
        return streamers(false);
    }

    default Stream<? extends PartitionStreamer> streamers(boolean immutable) {
        return streamersList(immutable).stream();
    }

    default Stream<LongSupplier> lineCounters() {
        return lineCountersList().stream();
    }

    default List<? extends PartitionStreamer> streamersList() {
        return streamersList(false);
    }

    List<? extends PartitionStreamer> streamersList(boolean immutable);

    List<LongSupplier> lineCountersList();
}
