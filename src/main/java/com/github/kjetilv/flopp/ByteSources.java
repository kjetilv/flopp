package com.github.kjetilv.flopp;

import java.io.Closeable;

public interface ByteSources extends Closeable {

    ByteSource source(Partition partition);

    @Override
    default void close() {
    }
}
