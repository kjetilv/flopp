package com.github.kjetilv.lopp;

import java.io.Closeable;
import java.util.function.ToLongFunction;

public interface Partitioned<T> extends Closeable {

    PartitionedStreams streams();

    PartitionedMapper mapper();

    PartitionedConsumer consumer();

    PartitionedProcessor processor(
        TempTargets<T> tempTargets,
        Transfers<T> transfer,
        ToLongFunction<T> sizer,
        LinesWriterFactory<T> linesWriterFactory
    );

    @Override
    void close();
}
