package com.github.kjetilv.flopp.kernel.files;

import java.io.Closeable;
import java.util.function.Supplier;

@FunctionalInterface
interface Transfer extends Runnable, Closeable, Supplier<Void> {

    @Override
    default Void get() {
        this.run();
        return null;
    }

    @Override
    default void close() {
    }
}
