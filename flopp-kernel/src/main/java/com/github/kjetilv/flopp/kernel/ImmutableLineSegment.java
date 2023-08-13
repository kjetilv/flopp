package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;

record ImmutableLineSegment(MemorySegment memorySegment, long startIndex, long endIndex)
    implements LineSegment, LineSegment.Immutable {

    @Override
    public LineSegment immutable() {
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
