package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.Partition;

import java.io.Closeable;

@FunctionalInterface
interface MemorySegmentSource extends Closeable {

    LineSegment get(Partition partition);

    @Override
    default void close() {}
}
