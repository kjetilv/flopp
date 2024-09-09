package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.io.LinesWriter;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

final class BitwisePartitionedConsumer implements PartitionedConsumer {

    private final PartitionedStreams streams;

    BitwisePartitionedConsumer(PartitionedStreams streams) {
        this.streams = streams;
    }

    @Override
    public Stream<CompletableFuture<PartitionResult<Void>>> forEachLine(
        BiConsumer<Partition, Stream<LineSegment>> consumer
    ) {
        return streams.streamers()
            .map(partitionStreamer ->
                CompletableFuture.supplyAsync(() -> new PartitionResult<>(
                    partitionStreamer.partition(),
                    getAccept(consumer, partitionStreamer)
                )));
    }

    private static Void getAccept(
        BiConsumer<Partition, Stream<LineSegment>> consumer,
        PartitionStreamer partitionStreamer
    ) {
        consumer.accept(partitionStreamer.partition(), partitionStreamer.lines());
        return null;
    }
}
