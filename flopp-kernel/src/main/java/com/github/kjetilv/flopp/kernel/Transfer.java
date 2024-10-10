package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.function.Supplier;

@FunctionalInterface
public interface Transfer extends Runnable, Closeable, Supplier<Void> {

    @Override
    default Void get() {
        this.run();
        return null;
    }

    @Override
    default void close() {
    }
}
