package com.github.kjetilv.lopp;

import java.io.Closeable;

public interface Transfer extends Runnable, Closeable {

    @Override
    default void close() {
    }
}
