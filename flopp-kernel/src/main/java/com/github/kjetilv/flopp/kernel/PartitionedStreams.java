package com.github.kjetilv.flopp.kernel;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface PartitionedStreams {

    Stream<LongSupplier> lineCounters();

    Stream<? extends PartitionStreamer> streamers();

    Stream<? extends CompletableFuture<PartitionStreamer>> streamers(ExecutorService executorService);
}
