package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;

record ImmutableSeparatedLine(
    MemorySegment memorySegment,
    int columnCount,
    long[] start,
    long[] end
) implements SeparatedLine {

    @Override
    public SeparatedLine immutableSeparatedLine() {
        return this;
    }

    @Override
    public long start(int column) {
        return start[column];
    }

    @Override
    public long end(int column) {
        return end[column];
    }
}
