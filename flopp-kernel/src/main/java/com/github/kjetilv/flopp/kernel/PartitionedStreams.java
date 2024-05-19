package com.github.kjetilv.flopp.kernel;

import java.util.function.LongSupplier;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface PartitionedStreams {

    Stream<LongSupplier> lineCounters();

    default Stream<? extends PartitionStreamer> streamers() {
        return streamers(false);
    }

    Stream<? extends PartitionStreamer> streamers(boolean immutable);
}
