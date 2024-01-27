package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.LineSegment;

import java.lang.foreign.MemorySegment;

public record ImmutableSliceLine(
    MemorySegment memorySegment,
    long length
) implements LineSegment {

    @Override
    public long offset() {
        return 0;
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{offset()}+\{length()}]";
    }

    @Override
    public LineSegment immutableSlice() {
        return this;
    }
}
