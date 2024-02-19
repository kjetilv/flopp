package com.github.kjetilv.flopp.kernel.bits;

import java.lang.foreign.MemorySegment;

@SuppressWarnings("unused")
record ImmutableSliceLine(MemorySegment memorySegment, long length)
    implements LineSegment {

    ImmutableSliceLine(MemorySegment memorySegment) {
        this(memorySegment, memorySegment.byteSize());
    }

    @Override
    public long startIndex() {
        return 0;
    }

    @Override
    public long endIndex() {
        return length;
    }

    @Override
    public LineSegment immutable() {
        return this;
    }

    @Override
    public LineSegment immutableSlice() {
        return this;
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{startIndex()}-\{endIndex()}]";
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || obj instanceof LineSegment lineSegment && lineSegment.asString().equals(asString());
    }

    @Override
    public int hashCode() {
        return asString().hashCode();
    }
}
