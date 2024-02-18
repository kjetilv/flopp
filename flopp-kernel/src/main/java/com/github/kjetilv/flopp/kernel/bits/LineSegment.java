package com.github.kjetilv.flopp.kernel.bits;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static com.github.kjetilv.flopp.kernel.bits.Bits.ALIGNMENT;
import static java.lang.foreign.ValueLayout.*;

@SuppressWarnings("unused")
public interface LineSegment {

    static LineSegment of(String string) {
        return of(string.getBytes(StandardCharsets.UTF_8));
    }

    public static LineSegment of(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(bytes.length);
        byteBuffer.put(bytes);
        byteBuffer.flip();
        MemorySegment memorySegment = MemorySegment.ofBuffer(byteBuffer);
        LineSegment lineSegment = of(memorySegment);
        return lineSegment;
    }

    static LineSegment of(MemorySegment memorySegment) {
        return new ImmutableSliceLine(memorySegment);
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

    static LineSegment of(long l) {
        return of(l, Math.toIntExact(ALIGNMENT));
    }

    static LineSegment of(long l, int len) {
        byte[] bytes = {
            (byte) (l & 0xFF),
            (byte) (l >> 8L & 0xFF),
            (byte) (l >> 16L & 0xFF),
            (byte) (l >> 24L & 0xFF),
            (byte) (l >> 32L & 0xFF),
            (byte) (l >> 40L & 0xFF),
            (byte) (l >> 48L & 0xFF),
            (byte) (l >> 56L & 0xFF)
        };
        return of(new String(bytes, 0, len));
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
        return new ImmutableSliceLine(lineNo(), slice, length);
    }

    default String asString() {
        return LineSegments.toString(this);
    }

    default String asString(int length) {
        return LineSegments.toString(this, length);
    }

    default long unalignedLongAt(long longOffset) {
        return longAt(longOffset, JAVA_LONG_UNALIGNED);
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

    default boolean isEndSafe() {
        return isAlignedAtEnd() || memorySegment().byteSize() - endIndex() > ALIGNMENT;
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

    default long getHeadLong() {
        return getHeadLong(startIndex() % ALIGNMENT);
    }

    default long getHeadLong(long head) {
        long l = memorySegment().get(JAVA_LONG, longStart());
        return l >> head * ALIGNMENT;
    }

    default long getLong(int longNo) {
        return memorySegment().get(JAVA_LONG, longStart() + longNo * 8L);
    }

    default long getTailLong() {
        long tail = endIndex() % ALIGNMENT;
        long l = memorySegment().get(JAVA_LONG, longEnd() - ALIGNMENT);
        return l & LineSegments.CLEAR_HEAD[Math.toIntExact(tail)];
    }

    default long getTail() {
        long endIndex = endIndex();
        long tail = endIndex % ALIGNMENT;
        return LineSegments.bytesAt(memorySegment(), longEnd(), tail);
    }

    default byte byteAt(long i) {
        return memorySegment().get(JAVA_BYTE, startIndex() + i);
    }

    default long bytesAt(long offset, long count) {
        return LineSegments.bytesAt(memorySegment(), index(offset), count);
    }

    default long index(long offset) {
        return startIndex() + offset;
    }

    private long longAt(long longOffset, OfLong javaLong) {
        try {
            return memorySegment().get(javaLong, startIndex() + longOffset);
        } catch (Exception e) {
            throw new IllegalStateException(
                STR."Alignment for \{longOffset}: \{(memorySegment().address() + longOffset) % ALIGNMENT}",
                e
            );
        }
    }
}
