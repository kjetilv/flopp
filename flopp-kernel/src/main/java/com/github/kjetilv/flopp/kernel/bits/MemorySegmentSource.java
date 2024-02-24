package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;

import java.io.Closeable;
import java.lang.foreign.MemorySegment;
import java.util.function.Function;

public interface MemorySegmentSource extends Function<Partition, MemorySegment>, Closeable {

    @Override
    void close();
}
