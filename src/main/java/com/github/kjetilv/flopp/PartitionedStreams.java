package com.github.kjetilv.flopp;

import java.util.stream.Stream;

public interface PartitionedStreams {

    Stream<Streamer> streamers();

    interface Streamer {

        Partition partition();

        Stream<NpLine> lines();
    }
}
