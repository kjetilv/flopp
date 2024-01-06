package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.function.Function;
import java.util.stream.Stream;

@SuppressWarnings("unused")
@FunctionalInterface
public interface PartitionedProcessor<T> extends Closeable {

    default void process(Function<T, String> processor) {
        processMulti(toMulti(processor));
    }

    void processMulti(Function<T, Stream<String>> processor);

    @Override
    default void close() {
    }

    private Function<T, Stream<String>> toMulti(Function<T, String> processor) {
        return line -> Stream.of(processor.apply(line));
    }
}
