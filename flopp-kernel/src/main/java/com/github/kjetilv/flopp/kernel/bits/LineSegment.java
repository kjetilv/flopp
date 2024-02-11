package com.github.kjetilv.flopp.kernel.bits;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

import static java.lang.foreign.ValueLayout.*;

@SuppressWarnings("unused")
public interface LineSegment {

    static LineSegment of(MemorySegment memorySegment) {
        return new ImmutableSliceLine(memorySegment);
    }

    static LineSegment of(String string) {
        return of(MemorySegment.ofArray(string.getBytes(StandardCharsets.UTF_8)));
    }

    static LineSegment of(MemorySegment memorySegment, long start, long end) {
        return new ImmutableLine(memorySegment, start, end);
    }

    static LineSegment ofRange(MemorySegment memorySegment, long startIndex, long endIndex) {
        return new ImmutableLine(memorySegment, startIndex, endIndex);
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

    default long longAt(int longOffset) {
        return memorySegment().get(JAVA_LONG_UNALIGNED, startIndex() + longOffset);
    }

    default byte byteAt(long i) {
        return memorySegment().get(JAVA_BYTE, startIndex() + i);
    }

    default long index(long pos) {
        return startIndex() + pos;
    }
}
