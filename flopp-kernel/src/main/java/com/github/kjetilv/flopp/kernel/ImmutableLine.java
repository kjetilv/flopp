package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.LineSegment;

import java.lang.foreign.MemorySegment;

public record ImmutableLine(
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
