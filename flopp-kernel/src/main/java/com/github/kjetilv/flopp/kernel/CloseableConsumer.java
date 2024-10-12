package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.function.Consumer;

@FunctionalInterface
public interface CloseableConsumer<T> extends Closeable, Consumer<T> {

    default void close() {
    }
}
