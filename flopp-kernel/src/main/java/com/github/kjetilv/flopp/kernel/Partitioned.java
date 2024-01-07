package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.*;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface Partitioned<P> extends Closeable {

    P partitioned();

    PartitionedStreams streams();

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

    PartitionedProcessor<ByteSeg> segmentProcessor(
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
    default <T> List<T> mapSegmentPartition(BiFunction<Partition, Stream<ByteSeg>, T> function) {
        try (PartitionedMapper mapper = mapper()) {
            return awaitCompleted(mapper
                .mapSegments(function)
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

    default void forEachNLinePartition(BiConsumer<Partition, Stream<NLine>> action) {
        try (PartitionedConsumer consumer = consumer()) {
            awaitCompleted(consumer.forEachNLine(action));
        }
    }

    default void forEachLine(Consumer<String> action) {
        try (PartitionedStreams streams = streams()) {
            streams.streamers()
                .forEach(streamer -> streamer.lines()
                    .forEach(action));
        }
    }

    default void forEachRawLine(Consumer<byte[]> action) {
        try (PartitionedStreams streams = streams()) {
            streams.streamers()
                .forEach(streamer -> streamer.rawLines()
                    .forEach(action));
        }
    }

    default void forEachNLine(Consumer<NLine> action) {
        try (PartitionedStreams streams = streams()) {
            streams.streamers()
                .forEach(streamer -> streamer.nLines()
                    .forEach(action));
        }
    }

    default void forEachRNLine(Consumer<RNLine> action) {
        try (PartitionedStreams streams = streams()) {
            streams.streamers()
                .forEach(streamer -> streamer.rnLines()
                    .forEach(action));
        }
    }

    default void forEachSegment(Consumer<ByteSeg> action) {
        try (PartitionedStreams streams = streams()) {
            streams.streamers()
                .forEach(streamer -> streamer.segments()
                    .forEach(action));
        }
    }

    @Override
    void close();

    private static <T> List<T> awaitCompleted(Stream<CompletableFuture<T>> futures) {
        return futures
            .toList()
            .stream()
            .map(CompletableFuture::join)
            .toList();
    }
}
