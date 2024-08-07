package com.github.kjetilv.flopp.kernel;

public sealed interface HashedLineSegment extends LineSegment, LineSegment.Immutable, LineSegment.Hashed
    permits HashedAlignedLineSegment, HashedUnalignedLineSegment {

    static LineSegment hash(LineSegment ls) {
        return hash(LineSegments.hashCode(ls), ls);
    }

    static LineSegment hash(int hash, LineSegment ls) {
        return hash(hash, ls, false);
    }

    static LineSegment hash(int hash, LineSegment ls, boolean aligned) {
        return new HashedAlignedLineSegment(hash, ls.memorySegment(), ls.startIndex(), ls.endIndex());
    }

    @Override
    default LineSegment immutable() {
        return this;
    }
}
