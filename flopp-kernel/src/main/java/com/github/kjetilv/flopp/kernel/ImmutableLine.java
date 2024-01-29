package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;

record ImmutableLine(
    MemorySegment memorySegment,
    long offset,
    long length
) implements LineSegment {

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{offset()}+\{length()}]";
    }

    @Override
    public LineSegment immutable() {
        return this;
    }
}
