package com.github.kjetilv.lopp;

import java.io.Closeable;
import java.util.function.Function;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface PartitionedProcessor extends Closeable {

    default void process(Function<String, String> processor) {
        processMulti(toMulti(processor));
    }

    void processMulti(Function<String, Stream<String>> processor);

    @Override
    default void close() {
    }

    private static Function<String, Stream<String>> toMulti(Function<String, String> processor) {
        return line -> Stream.of(processor.apply(line));
    }
}
