package com.github.kjetilv.flopp.kernel.bits;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static com.github.kjetilv.flopp.kernel.bits.Bits.ALIGNMENT;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

public interface LineSegment {

    static LineSegment of(String string) {
        return of(string.getBytes(StandardCharsets.UTF_8));
    }

    static LineSegment of(byte[] bytes) {
        return of(MemorySegment.ofBuffer(getByteBuffer(bytes)));
    }

    static LineSegment of(MemorySegment memorySegment) {
        return new ImmutableSliceLine(memorySegment);
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

    static LineSegment of(long l) {
        return of(l, Math.toIntExact(ALIGNMENT));
    }

    static LineSegment of(long l, int len) {
        return of(new String(
            new byte[] {
                (byte) (l & 0xFF),
                (byte) (l >> 8L & 0xFF),
                (byte) (l >> 16L & 0xFF),
                (byte) (l >> 24L & 0xFF),
                (byte) (l >> 32L & 0xFF),
                (byte) (l >> 40L & 0xFF),
                (byte) (l >> 48L & 0xFF),
                (byte) (l >> 56L & 0xFF)
            },
            0,
            len
        ));
    }

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
        return LineSegments.toString(this);
    }

    default String asString(int length) {
        return LineSegments.toString(this, length);
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

    default long head() {
        return head(startIndex() % ALIGNMENT);
    }

    default long head(long head) {
        long l = memorySegment().get(JAVA_LONG, longStart());
        return l >> head * ALIGNMENT;
    }

    default long getLong(int longNo) {
        return memorySegment().get(JAVA_LONG, longStart() + longNo * 8L);
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
        return LineSegments.bytesAt(memorySegment(), index(offset), count);
    }

    default long index(long offset) {
        return startIndex() + offset;
    }

    private static ByteBuffer getByteBuffer(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.allocateDirect(bytes.length);
        bb.put(bytes);
        bb.flip();
        return bb;
    }
}
