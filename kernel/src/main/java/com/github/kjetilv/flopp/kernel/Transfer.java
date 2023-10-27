package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;

public interface Transfer extends Runnable, Closeable {

    @Override
    default void close() {
    }
}
