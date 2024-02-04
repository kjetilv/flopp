package com.github.kjetilv.flopp.kernel.bits;

import java.lang.foreign.MemorySegment;

public record ImmutableLine(
    MemorySegment memorySegment,
    long offset,
    long length
) implements LineSegment {

    public ImmutableLine(MemorySegment memorySegment) {
        this(memorySegment, memorySegment.byteSize());
    }

    public ImmutableLine(MemorySegment memorySegment, long length) {
        this(memorySegment, 0, length);
    }

    @Override
    public LineSegment immutable() {
        return this;
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{offset()}+\{length()}]";
    }
}
