package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;
import java.nio.charset.Charset;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.fromEdgeLong;
import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.fromLongsWithinBounds;

record HashedLineSegment(int hash, MemorySegment memorySegment, long startIndex, long endIndex, boolean aligned)
    implements LineSegment, LineSegment.Immutable, LineSegment.Hashed {

    public static LineSegment hash(LineSegment ls) {
        return hash(LineSegments.hashCode(ls), ls);
    }

    public static LineSegment hash(int hash, LineSegment ls) {
        return hash(hash, ls, false);
    }

    public static LineSegment hash(int hash, LineSegment ls, boolean aligned) {
        return new HashedLineSegment(hash, ls.memorySegment(), ls.startIndex(), ls.endIndex(), aligned);
    }

    @Override
    public LineSegment immutable() {
        return this;
    }

    @Override
    public String asString(byte[] buffer, Charset charset) {
        return aligned
            ? fromLongsWithinBounds(memorySegment, startIndex, endIndex, buffer, charset)
            : fromEdgeLong(memorySegment, startIndex, endIndex, buffer, charset);
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
