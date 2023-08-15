package com.github.kjetilv.flopp;

import java.io.Closeable;

public interface Transfer extends Runnable, Closeable {

    @Override
    default void close() {
    }
}
