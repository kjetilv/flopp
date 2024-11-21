package com.github.kjetilv.flopp.kernel.segments;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.LineSegments;

import java.lang.foreign.MemorySegment;

public record ImmutableLineSegment(MemorySegment memorySegment, long startIndex, long endIndex)
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
