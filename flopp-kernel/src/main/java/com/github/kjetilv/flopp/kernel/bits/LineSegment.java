package com.github.kjetilv.flopp.kernel.bits;

import java.lang.foreign.MemorySegment;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

@SuppressWarnings("unused")
public interface LineSegment {

    static LineSegment ofRange(MemorySegment memorySegment, long startIndex, long endIndex) {
        return new ImmutableLine(memorySegment, startIndex, endIndex);
    }

    static LineSegment of(MemorySegment memorySegment, long start, long end) {
        return new ImmutableLine(memorySegment, start, end);
    }

    static String toString(MemorySegment segment, long start, long end) {
        return of(segment, start, end).asString();
    }

    MemorySegment memorySegment();

    long startIndex();

    long endIndex();

    default long length() {
        return endIndex() - startIndex();
    }

    default String tooString() {
        return LineSegments.toString(this);
    }

    default byte[] byteArray() {
        return LineSegments.toBytes(this);
    }

    default LineSegment immutable() {
        return new ImmutableLine(memorySegment(), startIndex(), endIndex());
    }

    default LineSegment immutableSlice() {
        return immutableSLice(endIndex());
    }

    default LineSegment immutableSLice(long length) {
        MemorySegment slice = memorySegment().asSlice(startIndex(), length);
        return new ImmutableSliceLine(slice, length);
    }

    default String asString() {
        return LineSegments.toString(this);
    }

    default String asString(int length) {
        return LineSegments.toString(this, length);
    }

    default byte byteAt(int i) {
        return memorySegment().get(JAVA_BYTE, startIndex() + i);
    }
}
