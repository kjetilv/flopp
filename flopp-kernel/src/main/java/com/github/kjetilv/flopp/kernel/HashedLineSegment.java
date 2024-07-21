package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;

record HashedLineSegment(int hash, MemorySegment memorySegment, long startIndex, long endIndex)
    implements LineSegment, LineSegment.Immutable, LineSegment.Hashed {

    public static LineSegment hash(LineSegment ls) {
        return hash(LineSegments.hashCode(ls), ls);
    }

    public static LineSegment hash(int hash, LineSegment ls) {
        return new HashedLineSegment(hash, ls.memorySegment(), ls.startIndex(), ls.endIndex());
    }

    @Override
    public LineSegment immutable() {
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof HashedLineSegment hashedLineSegment &&
               hash == hashedLineSegment.hash &&
               this.matches(hashedLineSegment);
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
