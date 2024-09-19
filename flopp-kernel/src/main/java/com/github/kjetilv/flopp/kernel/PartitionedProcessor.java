package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

@SuppressWarnings("unused")
@FunctionalInterface
public interface PartitionedProcessor<I, O> extends Closeable {

    default void process(Function<I, O> processor, ExecutorService executorService) {
        processFor(partition -> processor, executorService);
    }

    void processFor(Function<Partition, Function<I, O>> processor, ExecutorService executorService);

    @Override
    default void close() {
    }
}
