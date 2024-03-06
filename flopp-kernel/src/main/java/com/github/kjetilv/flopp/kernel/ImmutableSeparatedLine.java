package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;

public record ImmutableSeparatedLine(
    MemorySegment memorySegment,
    int columnCount,
    long[] start,
    long[] end
)
    implements SeparatedLine {

    @Override
    public SeparatedLine immutable() {
        return this;
    }
}
