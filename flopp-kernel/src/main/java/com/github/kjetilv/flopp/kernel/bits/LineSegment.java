package com.github.kjetilv.flopp.kernel.bits;

import java.lang.foreign.MemorySegment;

import static com.github.kjetilv.flopp.kernel.bits.Bits.ALIGNMENT;
import static java.lang.foreign.ValueLayout.*;

public interface LineSegment {

    MemorySegment memorySegment();

    long startIndex();

    long endIndex();

    default long length() {
        return endIndex() - startIndex();
    }

    @SuppressWarnings("unused")
    default LineSegment immutable() {
        return new ImmutableLine(memorySegment(), startIndex(), endIndex());
    }

    @SuppressWarnings("unused")
    default LineSegment immutableSlice() {
        return immutableSLice(endIndex());
    }

    default LineSegment immutableSLice(long length) {
        MemorySegment slice = memorySegment().asSlice(startIndex(), length);
        return new ImmutableSliceLine(slice, length);
    }

    default String asString() {
        return LineSegments.asString(this);
    }

    default String asString(int length) {
        return LineSegments.asString(this, length);
    }

    default long longCount() {
        return (longEnd() - longStart()) / ALIGNMENT;
    }

    default long headStart() {
        return startIndex() % ALIGNMENT;
    }

    default boolean isAlignedAtEnd() {
        return endIndex() % ALIGNMENT == 0;
    }

    default long longStart() {
        long startIndex = startIndex();
        long head = startIndex % ALIGNMENT;
        return startIndex - head;
    }

    default long longEnd() {
        long endIndex = endIndex();
        long tailEnd = endIndex % ALIGNMENT;
        return endIndex - tailEnd;
    }

    default long head(long head) {
        long l = memorySegment().get(JAVA_LONG, longStart());
        return l >> head * ALIGNMENT;
    }

    default long longNo(int longNo) {
        return memorySegment().get(JAVA_LONG, longStart() + longNo * 8L);
    }

    default long longAt(int longIndex) {
        return memorySegment().get(JAVA_LONG_UNALIGNED, longStart() + longIndex);
    }

    default long tail() {
        long endIndex = endIndex();
        long tail = endIndex % ALIGNMENT;
        return LineSegments.bytesAt(memorySegment(), longEnd(), tail);
    }

    default byte byteAt(long i) {
        return memorySegment().get(JAVA_BYTE, startIndex() + i);
    }

    default long bytesAt(long offset, long count) {
        return LineSegments.bytesAt(memorySegment(), startIndex() + offset, count);
    }
}
