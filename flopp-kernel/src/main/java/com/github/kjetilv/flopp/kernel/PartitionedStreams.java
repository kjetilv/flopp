package com.github.kjetilv.flopp.kernel;

import java.util.List;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

public interface PartitionedStreams {

    default Stream<? extends PartitionStreamer> streamers() {
        return streamers(false);
    }

    default Stream<? extends PartitionStreamer> streamers(boolean copying) {
        return streamersList(copying).stream();
    }

    default Stream<LongSupplier> lineCounters() {
        return lineCountersList().stream();
    }

    default List<? extends PartitionStreamer> streamersList() {
        return streamersList(false);
    }

    List<? extends PartitionStreamer> streamersList(boolean copying);

    List<LongSupplier> lineCountersList();
}
