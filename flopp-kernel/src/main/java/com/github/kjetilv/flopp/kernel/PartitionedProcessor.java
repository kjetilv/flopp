package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.partitions.Partition;

import java.io.Closeable;
import java.util.function.Function;
import java.util.function.Supplier;

@SuppressWarnings("unused")
public interface PartitionedProcessor<T, I, O> extends Closeable {

    default void forEach(Function<I, O> processor) {
        forEachPartition(partition -> processor);
    }

    default void forEachPartition(Supplier<Function<I, O>> processor) {
        forEachPartition(_ -> processor.get());
    }

    void forEachPartition(Function<Partition, Function<I, O>> processor);

    @Override
    default void close() {
    }
}
