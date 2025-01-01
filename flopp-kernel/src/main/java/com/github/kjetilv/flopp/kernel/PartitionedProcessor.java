package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.function.Function;
import java.util.function.Supplier;

@SuppressWarnings("unused")
public interface PartitionedProcessor<I, O> extends Closeable {

    default void forEach(Function<I, O> processor) {
        forEachPartition(partition -> processor);
    }

    default void forEachPartition(Supplier<Function<I, O>> processor) {
        forEachPartition(_ -> processor.get());
    }

    @Override
    default void close() {
    }

    void forEachPartition(Function<Partition, Function<I, O>> processor);
}
