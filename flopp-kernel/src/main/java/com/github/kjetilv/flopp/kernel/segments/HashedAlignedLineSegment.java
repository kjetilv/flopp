package com.github.kjetilv.flopp.kernel.segments;

import com.github.kjetilv.flopp.kernel.LineSegments;

import java.lang.foreign.MemorySegment;
import java.nio.charset.Charset;

import static com.github.kjetilv.flopp.kernel.MemorySegments.fromLongsWithinBounds;

record HashedAlignedLineSegment(int hash, MemorySegment memorySegment, long startIndex, long endIndex)
    implements HashedLineSegment {

    @Override
    public String asString(byte[] buffer, Charset charset) {
        return fromLongsWithinBounds(memorySegment, startIndex, endIndex, buffer, charset);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof HashedAlignedLineSegment hashedLineSegment &&
               hash == hashedLineSegment.hash &&
               this.matches(hashedLineSegment);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public String toString() {
        return LineSegments.toString(this);
    }
}
