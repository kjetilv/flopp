package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.MemorySegments;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.*;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface Partitioned<P> extends Closeable {

    P partitioned();

    List<Partition> partitions();

    PartitionedStreams streams();

    VectorPartitionedMapper vectorMapper();

    PartitionedMapper mapper();

    PartitionedConsumer consumer();

    PartitionedProcessor<String> processor(
        TempTargets<P> tempTargets,
        Transfers<P> transfer,
        ToIntFunction<P> sizer,
        LinesWriterFactory<P> linesWriterFactory
    );

    PartitionedProcessor<byte[]> bytesProcessor(
        TempTargets<P> tempTargets,
        Transfers<P> transfer,
        ToIntFunction<P> sizer,
        LinesWriterFactory<P> linesWriterFactory
    );

    PartitionedProcessor<NLine> nLineProcessor(
        TempTargets<P> tempTargets,
        Transfers<P> transfer,
        ToIntFunction<P> sizer,
        LinesWriterFactory<P> linesWriterFactory
    );

    PartitionedProcessor<RNLine> rnLineProcessor(
        TempTargets<P> tempTargets,
        Transfers<P> transfer,
        ToIntFunction<P> sizer,
        LinesWriterFactory<P> linesWriterFactory
    );

    PartitionedProcessor<ByteSeg> byteSegProcessor(
        TempTargets<P> tempTargets,
        Transfers<P> transfer,
        ToIntFunction<P> sizer,
        LinesWriterFactory<P> linesWriterFactory
    );

    PartitionedProcessor<Supplier<ByteSeg>> suppliedByteSegProcessor(
        TempTargets<P> tempTargets,
        Transfers<P> transfer,
        ToIntFunction<P> sizer,
        LinesWriterFactory<P> linesWriterFactory
    );

    default <T> List<T> mapPartition(BiFunction<Partition, Stream<String>, T> function) {
        try (PartitionedMapper mapper = mapper()) {
            return awaitCompleted(mapper
                .mapLines(function)
                .map(future ->
                    future.thenApply(PartitionResult::result)));
        }
    }

    default <T> List<T> mapBytesPartition(BiFunction<Partition, Stream<byte[]>, T> function) {
        try (PartitionedMapper mapper = mapper()) {
            return awaitCompleted(mapper
                .mapRawLines(function)
                .map(future ->
                    future.thenApply(PartitionResult::result)));
        }
    }

    default <T> List<T> mapNLinePartition(BiFunction<Partition, Stream<NLine>, T> function) {
        try (PartitionedMapper mapper = mapper()) {
            return awaitCompleted(mapper
                .mapNLines(function)
                .map(future ->
                    future.thenApply(PartitionResult::result)));
        }
    }

    default <T> List<T> mapRNLinePartition(BiFunction<Partition, Stream<RNLine>, T> function) {
        try (PartitionedMapper mapper = mapper()) {
            return awaitCompleted(mapper
                .mapRNLines(function)
                .map(future ->
                    future.thenApply(PartitionResult::result)));
        }
    }

    default <T> List<T> mapByteSegPartition(BiFunction<Partition, Stream<ByteSeg>, T> function) {
        try (PartitionedMapper mapper = mapper()) {
            return awaitCompleted(mapper
                .mapByteSegs(function)
                .map(future ->
                    future.thenApply(PartitionResult::result)));
        }
    }

    default <T> List<T> mapSuppliedByteSegPartition(BiFunction<Partition, Stream<Supplier<ByteSeg>>, T> function) {
        try (PartitionedMapper mapper = mapper()) {
            return awaitCompleted(mapper
                .mapSuppliedByteSegs(function)
                .map(future ->
                    future.thenApply(PartitionResult::result)));
        }
    }

    default <T> List<T> mapMemorySegmentPartition(
        BiFunction<Partition, Stream<MemorySegments.LineSegment>, T> function
    ) {
        try (VectorPartitionedMapper mapper = vectorMapper()) {
            return awaitCompleted(mapper
                .mapLines(function)
                .map(future ->
                    future.thenApply(PartitionResult::result)));
        }
    }

    default void forEachLine(Consumer<String> action) {
        forEach(action, PartitionedStreams.PartitionStreamer::lines);
    }

    default void forEachRawLine(Consumer<byte[]> action) {
        forEach(action, PartitionedStreams.PartitionStreamer::rawLines);
    }

    default void forEachNLine(Consumer<NLine> action) {
        forEach(action, PartitionedStreams.PartitionStreamer::nLines);
    }

    default void forEachRNLine(Consumer<RNLine> action) {
        forEach(action, PartitionedStreams.PartitionStreamer::rnLines);
    }

    default void forEachByteSeg(Consumer<ByteSeg> action) {
        forEach(action, PartitionedStreams.PartitionStreamer::byteSegs);
    }

    default void forEachLinePartition(BiConsumer<Partition, Stream<String>> action) {
        try (PartitionedConsumer consumer = consumer()) {
            awaitCompleted(consumer.forEachLine(action));
        }
    }

    default void forEachBytesPartition(BiConsumer<Partition, Stream<byte[]>> action) {
        try (PartitionedConsumer consumer = consumer()) {
            awaitCompleted(consumer.forEachRawLine(action));
        }
    }

    default void forEachNLinePartition(BiConsumer<Partition, Stream<NLine>> action) {
        try (PartitionedConsumer consumer = consumer()) {
            awaitCompleted(consumer.forEachNLine(action));
        }
    }

    default void forEachRNLinePartition(BiConsumer<Partition, Stream<RNLine>> action) {
        try (PartitionedConsumer consumer = consumer()) {
            awaitCompleted(consumer.forEachRNLine(action));
        }
    }

    default void forEachByteSegPartition(BiConsumer<Partition, Stream<ByteSeg>> action) {
        try (PartitionedConsumer consumer = consumer()) {
            awaitCompleted(consumer.forEachByteSeg(action));
        }
    }

    @Override
    default void close() {
    }

    private <B> void forEach(Consumer<B> action, Function<PartitionedStreams.PartitionStreamer, Stream<B>> fun) {
        try (PartitionedStreams streams = streams()) {
            streams.streamers()
                .forEach(streamer -> fun.apply(streamer)
                    .forEach(action));
        }
    }

    private static <T> List<T> awaitCompleted(Stream<CompletableFuture<T>> futures) {
        return futures
            .toList()
            .stream()
            .map(CompletableFuture::join)
            .toList();
    }
}
