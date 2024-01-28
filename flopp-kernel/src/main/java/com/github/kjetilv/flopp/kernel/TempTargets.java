package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.io.IOException;

@FunctionalInterface
public interface TempTargets<T> extends Closeable {

    T temp(Partition partition);

    @Override
    default void close() {

    }
}
