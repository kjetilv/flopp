package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;

import java.io.Closeable;

@FunctionalInterface
interface MemorySegmentSource extends Closeable {

    LineSegment get(Partition partition);

    @Override
    default void close() {}
}
