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

        Stream<String> lines();

        Stream<byte[]> rawLines();

        Stream<NLine> nLines();

        Stream<RNLine> rnLines();

        Stream<ByteSegPartitionSpliterator.ByteSeg> segments();
    }
}
