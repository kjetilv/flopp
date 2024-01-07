package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface PartitionedMapper extends Closeable {

    <T> Stream<CompletableFuture<PartitionResult<T>>> mapLines(
        BiFunction<Partition, Stream<String>, T> processor
    );

    <T> Stream<CompletableFuture<PartitionResult<T>>> mapRawLines(
        BiFunction<Partition, Stream<byte[]>, T> processor
    );

    <T> Stream<CompletableFuture<PartitionResult<T>>> mapNLines(
        BiFunction<Partition, Stream<NLine>, T> processor
    );

    <T> Stream<CompletableFuture<PartitionResult<T>>> mapRNLines(
        BiFunction<Partition, Stream<RNLine>, T> processor
    );

    <T> Stream<CompletableFuture<PartitionResult<T>>> mapSegments(
        BiFunction<Partition, Stream<ByteSeg>, T> processor
    );

    @Override
    default void close() {
    }
}
