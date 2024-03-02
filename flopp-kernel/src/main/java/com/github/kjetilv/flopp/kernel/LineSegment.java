package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;

public interface LineSegment extends Range {

    MemorySegment memorySegment();

    @SuppressWarnings("unused")
    default LineSegment immutable() {
        return new LineSegments.Immutable(memorySegment(), startIndex(), endIndex());
    }

    @SuppressWarnings("unused")
    default LineSegment immutableSlice() {
        return immutableSLice(endIndex());
    }

    default LineSegment immutableSLice(long length) {
        return new LineSegments.ImmutableSlice(
            memorySegment().asSlice(startIndex(), length),
            length
        );
    }

    default String asString() {
        return LineSegments.asString(this);
    }

    default String asString(int length) {
        return LineSegments.asString(this, length);
    }

    default long longCount() {
        return (longEnd() - longStart()) / LineSegments.ALIGNMENT;
    }

    default long headStart() {
        return startIndex() % LineSegments.ALIGNMENT;
    }

    default boolean isAlignedAtEnd() {
        return endIndex() % LineSegments.ALIGNMENT == 0;
    }

    default long longStart() {
        long startIndex = startIndex();
        long head = startIndex % LineSegments.ALIGNMENT;
        return startIndex - head;
    }

    default long longEnd() {
        long endIndex = endIndex();
        long tailEnd = endIndex % LineSegments.ALIGNMENT;
        return endIndex - tailEnd;
    }

    default long head(long head) {
        long l = memorySegment().get(LineSegments.LONG, longStart());
        return l >> head * LineSegments.ALIGNMENT;
    }

    default long longNo(int longNo) {
        return memorySegment().get(LineSegments.LONG, longStart() + longNo * LineSegments.ALIGNMENT);
    }

    @SuppressWarnings("unused")
    default long longAt(int longIndex) {
        return memorySegment().get(JAVA_LONG_UNALIGNED, longStart() + longIndex);
    }

    default long tail() {
        long endIndex = endIndex();
        long tail = endIndex % LineSegments.ALIGNMENT;
        return LineSegments.bytesAt(memorySegment(), longEnd(), tail);
    }

    default byte byteAt(long i) {
        return memorySegment().get(JAVA_BYTE, startIndex() + i);
    }

    default long bytesAt(long offset, long count) {
        return LineSegments.bytesAt(memorySegment(), startIndex() + offset, count);
    }

}
