package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;

public interface Transfers<T> extends Closeable {

    Transfer transfer(Partition partition, T result);

    @Override
    default void close() {
    }
}
