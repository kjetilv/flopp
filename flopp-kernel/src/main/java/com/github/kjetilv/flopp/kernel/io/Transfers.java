package com.github.kjetilv.flopp.kernel.io;

import com.github.kjetilv.flopp.kernel.Partition;

import java.io.Closeable;

@FunctionalInterface
public interface Transfers<T> extends Closeable {

    Transfer transfer(Partition partition, T source);

    @Override
    default void close() {
    }
}
