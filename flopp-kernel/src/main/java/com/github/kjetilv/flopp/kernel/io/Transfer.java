package com.github.kjetilv.flopp.kernel.io;

import java.io.Closeable;

@FunctionalInterface
public interface Transfer extends Runnable, Closeable {

    @Override
    default void close() {
    }
}
