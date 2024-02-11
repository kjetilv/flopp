package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

@SuppressWarnings("unused")
@FunctionalInterface
public interface PartitionedProcessor<T> extends Closeable {

    void process(
        Function<T, String> processor,
        ExecutorService executorService
    );

    @Override
    default void close() {
    }
}
