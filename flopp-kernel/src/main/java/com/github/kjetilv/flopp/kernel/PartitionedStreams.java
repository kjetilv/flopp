package com.github.kjetilv.flopp.kernel;

import java.util.function.LongSupplier;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface PartitionedStreams {

    Stream<LongSupplier> lineCounters();

    Stream<? extends PartitionStreamer> streamers();
}
