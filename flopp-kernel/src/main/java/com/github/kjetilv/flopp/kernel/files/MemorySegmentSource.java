package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.partitions.Partition;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;

import java.io.Closeable;

@FunctionalInterface
interface MemorySegmentSource extends Closeable {

    @Override
    default void close() {
    }

    LineSegment get(Partition partition);
}
