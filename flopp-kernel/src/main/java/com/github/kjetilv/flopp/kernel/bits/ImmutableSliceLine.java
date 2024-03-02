package com.github.kjetilv.flopp.kernel.bits;

import java.lang.foreign.MemorySegment;

record ImmutableSliceLine(MemorySegment memorySegment, long length)
    implements LineSegment {

    ImmutableSliceLine(MemorySegment memorySegment) {
        this(memorySegment, memorySegment.byteSize());
    }

    @Override
    public long startIndex() {
        return 0;
    }

    @Override
    public long endIndex() {
        return length;
    }

    @Override
    public LineSegment immutable() {
        return this;
    }

    @Override
    public LineSegment immutableSlice() {
        return this;
    }

    @Override
    public String toString() {
        return LineSegments.toString(this);
    }
}
