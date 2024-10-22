package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.partitions.Partition;

import java.io.Closeable;

@FunctionalInterface
public interface Transfers<T> extends Closeable {

    @Override
    default void close() {
    }

    Transfer transfer(Partition partition, T source);
}
