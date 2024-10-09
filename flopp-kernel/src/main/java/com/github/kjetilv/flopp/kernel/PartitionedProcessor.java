package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.function.Function;

@SuppressWarnings("unused")
@FunctionalInterface
public interface PartitionedProcessor<I, O> extends Closeable {

    default void process(Function<I, O> processor) {
        processFor(partition -> processor);
    }

    void processFor(Function<Partition, Function<I, O>> processor);

    @Override
    default void close() {
    }
}
