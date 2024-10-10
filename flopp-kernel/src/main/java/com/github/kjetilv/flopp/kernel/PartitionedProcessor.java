package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.function.Function;

@SuppressWarnings("unused")
@FunctionalInterface
public interface PartitionedProcessor<T, I, O> extends Closeable {

    default void process(T target, Function<I, O> processor) {
        processFor(target, partition -> processor);
    }

    void processFor(T target, Function<Partition, Function<I, O>> processor);

    @Override
    default void close() {
    }
}
