package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;

@SuppressWarnings("unused")
public interface LineSegment {

    MemorySegment memorySegment();

    long offset();

    long length();

    default LineSegment validate() {
        if (length() < 0) {
            throw new IllegalStateException(STR."\{this} has negative length");
        }
        return this;
    }

    default String tooString() {
        return LineSegments.toString(this);
    }

    default byte[] byteArray() {
        return LineSegments.toBytes(this);
    }

    default LineSegment immutable() {
        return new ImmutableLine(memorySegment(), offset(), length());
    }

    default LineSegment immutableSlice() {
        MemorySegment slice = memorySegment().asSlice(offset(), length());
        return new ImmutableSliceLine(slice, length());
    }

    default String asString() {
        return LineSegments.toString(this);
    }
}
