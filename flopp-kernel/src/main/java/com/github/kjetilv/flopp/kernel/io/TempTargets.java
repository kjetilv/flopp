package com.github.kjetilv.flopp.kernel.io;

import com.github.kjetilv.flopp.kernel.Partition;

import java.io.Closeable;

@FunctionalInterface
public interface TempTargets<T> extends Closeable {

    T temp(Partition partition);

    @Override
    default void close() {
    }
}
