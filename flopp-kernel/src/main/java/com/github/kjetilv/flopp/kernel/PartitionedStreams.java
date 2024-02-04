package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

public interface PartitionedStreams extends Closeable {

    default Stream<? extends PartitionStreamer> streamers() {
        return streamersList().stream();
    }

    default Stream<LongSupplier> lineCounters() {
        return lineCountersList().stream();
    }

    List<? extends PartitionStreamer> streamersList();

    List<LongSupplier> lineCountersList();

    @Override
    void close();
}
