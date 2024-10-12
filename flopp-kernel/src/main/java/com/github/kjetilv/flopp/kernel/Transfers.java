package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.partitions.Partition;

import java.io.Closeable;

@FunctionalInterface
public interface Transfers<T> extends Closeable {

    Transfer transfer(Partition partition, T source);

    @Override
    default void close() {
    }
}
