package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

@SuppressWarnings("unused")
public interface LineSegment {

    static LineSegment of(MemorySegment memorySegment, long offset, long length) {
        return new ImmutableLine(memorySegment, offset, length);
    }

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
        return immutableSLice(length());
    }

    default LineSegment immutableSLice(long length) {
        MemorySegment slice = memorySegment().asSlice(offset(), length);
        return new ImmutableSliceLine(slice, length);
    }

    default String asString() {
        return LineSegments.toString(this);
    }

    default String asString(int length) {
        return LineSegments.toString(this, length);
    }

    default byte byteAt(int i ) {
        return memorySegment().get(JAVA_BYTE, offset() + i);
    }
}
