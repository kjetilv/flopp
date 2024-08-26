package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.segments.LineSegment;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface PartitionedConsumer {

    Stream<CompletableFuture<PartitionResult<Void>>> forEachLine(
        BiConsumer<Partition, Stream<LineSegment>> consumer
    );
}
