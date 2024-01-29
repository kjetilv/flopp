package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;

record ImmutableSliceLine(
    MemorySegment memorySegment,
    long length
) implements LineSegment {

    @Override
    public long offset() {
        return 0;
    }

    @Override
    public LineSegment immutableSlice() {
        return this;
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{offset()}+\{length()}]";
    }
}
