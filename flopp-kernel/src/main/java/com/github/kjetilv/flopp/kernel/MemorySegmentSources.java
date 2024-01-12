package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;

public interface MemorySegmentSources extends Closeable {

    MemorySegmentSource source(Partition partition);

    @Override
    default void close() {
    }
}
