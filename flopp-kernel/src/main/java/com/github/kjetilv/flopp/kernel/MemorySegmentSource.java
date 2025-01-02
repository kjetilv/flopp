package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;

@FunctionalInterface
public interface MemorySegmentSource extends Closeable {

    @Override
    default void close() {
    }

    LineSegment get(Partition partition);
}
