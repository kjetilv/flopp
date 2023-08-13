package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;

record ImmutableSliceSegment(MemorySegment memorySegment, long length)
    implements LineSegment, LineSegment.Immutable {

    ImmutableSliceSegment(MemorySegment memorySegment) {
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
    public boolean equals(Object obj) {
        return obj instanceof LineSegment lineSegment && this.matches(lineSegment);
    }

    @Override
    public int hashCode() {
        return LineSegments.hashCode(this);
    }

    @Override
    public String toString() {
        return LineSegments.toString(this);
    }
}
