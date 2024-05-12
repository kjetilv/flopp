package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;

record ImmutableLineSegment(MemorySegment memorySegment, long startIndex, long endIndex)
    implements LineSegment {

    @Override
    public LineSegment immutable() {
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof LineSegment other && LineSegments.equals(this, other);
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
