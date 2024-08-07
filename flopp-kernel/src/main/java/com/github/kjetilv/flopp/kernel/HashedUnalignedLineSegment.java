package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;
import java.nio.charset.Charset;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.fromEdgeLong;

record HashedUnalignedLineSegment(int hash, MemorySegment memorySegment, long startIndex, long endIndex, boolean aligned)
    implements HashedLineSegment {

    @Override
    public String asString(byte[] buffer, Charset charset) {
        return fromEdgeLong(memorySegment, startIndex, endIndex, buffer, charset);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof HashedUnalignedLineSegment hashedLineSegment &&
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
