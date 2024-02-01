package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.stream.Stream;

public interface PartitionedStreams extends Closeable {

    Stream<? extends PartitionStreamer> streamers();

    @Override
    default void close() {
    }

    interface PartitionStreamer {

        Stream<LineSegment> lines();

        Partition partition();
    }
}
