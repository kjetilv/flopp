package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;

import java.io.Closeable;
import java.lang.foreign.MemorySegment;

@FunctionalInterface
interface MemorySegmentSource extends Closeable {

    MemorySegment get(Partition partition);

    @Override
    default void close() {}
}
