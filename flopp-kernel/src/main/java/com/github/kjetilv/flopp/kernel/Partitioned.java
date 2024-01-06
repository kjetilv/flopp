package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface Partitioned<T> extends Closeable {

    T partitioned();

    PartitionedStreams streams();

    PartitionedMapper mapper();

    PartitionedConsumer consumer();

    PartitionedProcessor<String> processor(
        TempTargets<T> tempTargets,
        Transfers<T> transfer,
        ToLongFunction<T> sizer,
        LinesWriterFactory<T> linesWriterFactory
    );

    PartitionedProcessor<byte[]> bytesProcessor(
        TempTargets<T> tempTargets,
        Transfers<T> transfer,
        ToLongFunction<T> sizer,
        LinesWriterFactory<T> linesWriterFactory
    );

    PartitionedProcessor<NLine> nLineProcessor(
        TempTargets<T> tempTargets,
        Transfers<T> transfer,
        ToLongFunction<T> sizer,
        LinesWriterFactory<T> linesWriterFactory
    );

    PartitionedProcessor<RNLine> rnLineProcessor(
        TempTargets<T> tempTargets,
        Transfers<T> transfer,
        ToLongFunction<T> sizer,
        LinesWriterFactory<T> linesWriterFactory
    );

    default List<T> mapPartition(BiFunction<Partition, Stream<NLine>, T> function) {
        try (PartitionedMapper mapper = mapper()) {
            return Futures.awaitCompleted(mapper
                .mapNLines(function)
                .map(future ->
                    future.thenApply(PartitionResult::result)));
        }
    }

    default void forEachPartition(BiConsumer<Partition, Stream<NLine>> action) {
        try (PartitionedConsumer consumer = consumer()) {
            Futures.awaitCompleted(consumer.forEachNLine(action));
        }
    }

    default void forEachLine(Consumer<String> action) {
        try (PartitionedStreams streams = streams()) {
            streams.streamers()
                .forEach(streamer -> streamer.lines()
                    .forEach(action));
        }
    }

    default void forEachNumberedLine(Consumer<NLine> action) {
        try (PartitionedStreams streams = streams()) {
            streams.streamers()
                .forEach(streamer -> streamer.nLines()
                    .forEach(action));
        }
    }

    @Override
    void close();
}
