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
        return new ImmutableLine(1, memorySegment, start, end);
    }

    static LineSegment ofRange(MemorySegment memorySegment, long startIndex, long endIndex) {
        return new ImmutableLine(1, memorySegment, startIndex, endIndex);
    }

    static String toString(MemorySegment segment, long start, long end) {
        return of(segment, start, end).asString();
    }

    long lineNo();

    MemorySegment memorySegment();

    long startIndex();

    long endIndex();

    default long length() {
        return endIndex() - startIndex();
    }

    default byte[] byteArray() {
        return LineSegments.toBytes(this);
    }

    default LineSegment immutable() {
        return new ImmutableLine(lineNo(), memorySegment(), startIndex(), endIndex());
    }

    default LineSegment immutableSlice() {
        return immutableSLice(endIndex());
    }

    default LineSegment immutableSLice(long length) {
        MemorySegment slice = memorySegment().asSlice(startIndex(), length);
        return new ImmutableSliceLine(lineNo(),slice, length);
    }

    default String asString() {
        return LineSegments.toString(this);
    }

    default String asString(int length) {
        return LineSegments.toString(this, length);
    }

    default int misalignedStart() {
        return Math.toIntExact(ALIGNMENT - startIndex() % ALIGNMENT);
    }

    default long longAt(int longOffset) {
        return longAt(longOffset, JAVA_LONG);
    }

    default long unalignedLongAt(int longOffset) {
        return longAt(longOffset, JAVA_LONG_UNALIGNED);
    }

    private long longAt(int longOffset, OfLong javaLong) {
        try {
            return memorySegment().get(javaLong, startIndex() + longOffset);
        } catch (Exception e) {
            throw new IllegalStateException(
                STR."Alignment for \{longOffset}: \{(memorySegment().address() + longOffset) % ALIGNMENT}",
                e);
        }
    }

    default byte byteAt(long i) {
        return memorySegment().get(JAVA_BYTE, startIndex() + i);
    }

    default long bytesAt(long offset, int count) {
        return LineSegments.bytesAt(memorySegment(), index(offset), count);

    }

    default long index(long offset) {
        return startIndex() + offset;
    }

    long ALIGNMENT = JAVA_LONG.byteAlignment();

}
