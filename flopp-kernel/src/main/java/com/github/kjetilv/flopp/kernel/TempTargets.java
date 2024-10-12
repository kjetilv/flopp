package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.partitions.Partition;

import java.io.Closeable;

@FunctionalInterface
public interface TempTargets<T> extends Closeable {

    T temp(Partition partition);

    @Override
    default void close() {
    }
}
