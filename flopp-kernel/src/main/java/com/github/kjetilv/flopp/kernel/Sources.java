package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;

public interface Sources extends Closeable {

    MemorySegmentSources memorySegmentSources();

    ByteSources byteSources();

    @Override
    void close();
}
