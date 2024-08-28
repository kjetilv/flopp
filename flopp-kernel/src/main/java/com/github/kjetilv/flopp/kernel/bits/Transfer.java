package com.github.kjetilv.flopp.kernel.bits;

import java.io.Closeable;

@FunctionalInterface
public interface Transfer extends Runnable, Closeable {

    @Override
    default void close() {
    }
}
