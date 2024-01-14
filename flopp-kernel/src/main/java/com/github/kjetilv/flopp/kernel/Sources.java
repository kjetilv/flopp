package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;

public interface Sources extends Closeable {

    public MemorySegmentSources memorySegmentSources();

    public ByteSources byteSources();

    @Override
    default void close() {
    }
}
