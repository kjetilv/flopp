package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;

@FunctionalInterface
public interface Transfers<T> extends Closeable {

    Transfer transfer(Partition partition, T source);

    @Override
    default void close() {
    }
}
