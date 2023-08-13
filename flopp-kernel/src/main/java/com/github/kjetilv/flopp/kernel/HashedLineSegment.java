package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;

record HashedLineSegment(MemorySegment memorySegment, long startIndex, long endIndex, int hash)
    implements LineSegment, LineSegment.Immutable, LineSegment.Hashed {

    public static LineSegment hash(LineSegment ls) {
        return hash(ls, LineSegments.hashCode(ls));
    }

    public static LineSegment hash(LineSegment ls, int hash) {
        return new HashedLineSegment(ls.memorySegment(), ls.startIndex(), ls.endIndex(), hash);
    }

    @Override
    public LineSegment hashed() {
        return this;
    }

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
        return hash;
    }

    @Override
    public String toString() {
        return LineSegments.toString(this);
    }
}
