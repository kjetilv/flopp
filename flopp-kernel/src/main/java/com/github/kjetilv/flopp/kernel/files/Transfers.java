package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.partitions.Partition;

import java.io.Closeable;

@FunctionalInterface
interface Transfers<T> extends Closeable {

    @Override
    default void close() {
    }

    Transfer transfer(Partition partition, T source);
}
