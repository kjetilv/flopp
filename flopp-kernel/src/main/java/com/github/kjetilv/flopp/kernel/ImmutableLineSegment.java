package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;

record ImmutableLineSegment(MemorySegment memorySegment, long startIndex, long endIndex)
    implements LineSegment {

    @Override
    public LineSegment immutable() {
        return this;
    }

    @Override
    public String toString() {
        return LineSegments.toString(this);
    }
}
