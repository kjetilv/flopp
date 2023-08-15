package com.github.kjetilv.lopp;

import java.io.Closeable;

public interface ByteSources extends Closeable {

    ByteSource source(Partition partition);

    @Override
    default void close() {
    }
}
