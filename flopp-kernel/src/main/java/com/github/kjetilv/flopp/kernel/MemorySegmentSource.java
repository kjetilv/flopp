package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.MemorySegment;

public interface MemorySegmentSource extends Closeable {

    MemorySegment get();

    @Override
    default void close() {
    }
}
