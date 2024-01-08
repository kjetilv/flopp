package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface PartitionedConsumer extends Closeable {

    Stream<CompletableFuture<PartitionResult<Void>>> forEachLine(
        BiConsumer<Partition, Stream<String>> consumer
    );

    Stream<CompletableFuture<PartitionResult<Void>>> forEachRawLine(
        BiConsumer<Partition, Stream<byte[]>> consumer
    );

    Stream<CompletableFuture<PartitionResult<Void>>> forEachNLine(
        BiConsumer<Partition, Stream<NLine>> consumer
    );

    Stream<CompletableFuture<PartitionResult<Void>>> forEachRNLine(
        BiConsumer<Partition, Stream<RNLine>> consumer
    );

    Stream<CompletableFuture<PartitionResult<ByteSeg>>> forEachSegment(
        BiConsumer<Partition, Stream<ByteSeg>> consumer
    );

    Stream<CompletableFuture<PartitionResult<Supplier<ByteSeg>>>> forEachSuppliedSegment(
        BiConsumer<Partition, Stream<Supplier<ByteSeg>>> consumer
    );

    @Override
    default void close() {
    }
}
