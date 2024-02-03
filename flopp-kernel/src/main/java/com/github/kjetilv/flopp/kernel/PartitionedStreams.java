package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.List;
import java.util.stream.Stream;

public interface PartitionedStreams extends Closeable {

    default Stream<? extends PartitionStreamer> streamers() {
        return streamersList().stream();
    }

    List<? extends PartitionStreamer> streamersList();

    @Override
    default void close() {
    }
}
