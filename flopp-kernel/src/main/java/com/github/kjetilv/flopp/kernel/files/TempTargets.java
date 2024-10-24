package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.partitions.Partition;

import java.io.Closeable;

@FunctionalInterface
interface TempTargets<T> extends Closeable {

    @Override
    default void close() {
    }

    T temp(Partition partition);
}
