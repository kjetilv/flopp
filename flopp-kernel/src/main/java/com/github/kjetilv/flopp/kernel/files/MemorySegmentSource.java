package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.Partition;

import java.io.Closeable;

@FunctionalInterface
interface MemorySegmentSource extends Closeable {

    @Override
    default void close() {
    }

    LineSegment get(Partition partition);
}
