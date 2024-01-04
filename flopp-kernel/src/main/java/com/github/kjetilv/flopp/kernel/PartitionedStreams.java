package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.stream.Stream;

@FunctionalInterface
public interface PartitionedStreams extends Closeable {
    Stream<Streamer> streamers();

    @Override
    default void close() {
    }

    interface Streamer {

        Partition partition();

        Stream<NLine> nLines();

        Stream<RNLine> rawNLines();

        Stream<byte[]> rawByteLines();

        Stream<String> lines();
    }
}
