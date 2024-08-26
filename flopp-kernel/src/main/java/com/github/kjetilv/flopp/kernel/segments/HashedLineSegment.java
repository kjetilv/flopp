package com.github.kjetilv.flopp.kernel.segments;

import com.github.kjetilv.flopp.kernel.segments.LineSegment.Hashed;
import com.github.kjetilv.flopp.kernel.segments.LineSegment.Immutable;

public sealed interface HashedLineSegment
    extends LineSegment, Immutable, Hashed
    permits HashedAlignedLineSegment, HashedUnalignedLineSegment {

    static LineSegment hash(int hash, LineSegment ls, boolean aligned) {
        return aligned
            ? alignedHash(hash, ls)
            : unalignedHash(hash, ls);
    }

    static LineSegment unalignedHash(int hash, LineSegment ls) {
        return new HashedUnalignedLineSegment(hash, ls.memorySegment(), ls.startIndex(), ls.endIndex());
    }

    static LineSegment alignedHash(int hash, LineSegment ls) {
        return new HashedAlignedLineSegment(hash, ls.memorySegment(), ls.startIndex(), ls.endIndex());
    }

    @Override
    default LineSegment immutable() {
        return this;
    }
}
