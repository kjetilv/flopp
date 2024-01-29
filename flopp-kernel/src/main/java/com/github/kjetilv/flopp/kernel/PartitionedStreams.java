package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.stream.Stream;

public interface PartitionedStreams extends Closeable {

    Stream<PartitionStreamer> streamers();

    @Override
    default void close() {
    }

    interface PartitionStreamer extends Closeable {

        Stream<LineSegment> lines();

        @Override
        default void close() {
        }

        Partition partition();
    }
}
