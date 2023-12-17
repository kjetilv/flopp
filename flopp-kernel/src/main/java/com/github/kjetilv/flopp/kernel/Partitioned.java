package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface Partitioned<T> extends Closeable {

    Supplier<Long> lineCounter();

    PartitionedStreams streams();

    PartitionedMapper mapper();

    PartitionedConsumer consumer();

    PartitionedProcessor processor(
        TempTargets<T> tempTargets,
        Transfers<T> transfer,
        ToLongFunction<T> sizer,
        LinesWriterFactory<T> linesWriterFactory
    );

    default void forEachPartition(BiConsumer<Partition, Stream<NpLine>> action) {
        try (PartitionedConsumer consumer = consumer()) {
            consumer.forEach(action);
        }
    }

    default void forEachLine(Consumer<String> action) {
        forEachNumberedLine(npLine -> action.accept(npLine.line()));
    }

    default void forEachNumberedLine(Consumer<NpLine> action) {
        try (PartitionedStreams streams = streams()) {
            streams.streamers()
                .forEach(streamer -> streamer.lines()
                    .forEach(action));
        }
    }

    @Override
    void close();
}
