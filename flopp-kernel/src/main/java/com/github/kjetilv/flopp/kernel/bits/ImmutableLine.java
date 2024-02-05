package com.github.kjetilv.flopp.kernel.bits;

import java.lang.foreign.MemorySegment;

record ImmutableLine(
    MemorySegment memorySegment,
    long offset,
    long length
) implements LineSegment {

    @Override
    public LineSegment immutable() {
        return this;
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{offset()}+\{length()}]";
    }
}
