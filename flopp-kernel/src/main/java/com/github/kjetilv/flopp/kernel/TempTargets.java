package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.partitions.Partition;

import java.io.Closeable;

@FunctionalInterface
public interface TempTargets<T> extends Closeable {

    @Override
    default void close() {
    }

    T temp(Partition partition);
}
