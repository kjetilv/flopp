package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;

import static com.github.kjetilv.flopp.kernel.LineSegments.LAYOUT;
import static com.github.kjetilv.flopp.kernel.LineSegments.UNALIGNED_LAYOUT;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

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

    default boolean hasRange(int startIndex, int endIndex) {
        return startIndex() == startIndex && endIndex() == endIndex;
    }

    default String asString() {
        return LineSegments.asString(this);
    }

    default String asString(int length) {
        return LineSegments.asString(this, length);
    }

    /**
     * The aligned start of this segment. May be ahead of/less-than {@link #startIndex() start index}.
     * @return Aligned start
     */
    default long alignedStart() {
        return startIndex() - headStart();
    }

    /**
     * The aligned end of this segment. May be ahead of/less-than {@link #endIndex() end index}.
     * @return Aligned end
     */
    default long alignedEnd() {
        long endIndex = endIndex();
        long tailEnd = endIndex % ALIGNMENT;
        return endIndex - tailEnd;
    }

    default long alignedCount() {
        return (alignedEnd() - alignedStart()) / ALIGNMENT;
    }

    default long fullLongCount() {
        return (alignedEnd() - firstAlignedStart()) / ALIGNMENT;
    }

    default long headStart() {
        return startIndex() % ALIGNMENT;
    }

    default boolean isAlignedAtStart() {
        return headStart() == 0L;
    }

    default boolean isAlignedAtEnd() {
        return endIndex() % ALIGNMENT == 0;
    }

    default long firstAlignedStart() {
        return startIndex() + headLength();
    }

    default int headLength() {
        long head = startIndex() % ALIGNMENT;
        long padding = head == 0L ? 0L : ALIGNMENT - head;
        return Math.toIntExact(padding);
    }

    default long head() {
        return bytesAt(0, headLength());
    }

    default long head(long head) {
        long l = memorySegment().get(LAYOUT, alignedStart());
        return l >> head * ALIGNMENT;
    }

    default long longNo(int longNo) {
        return memorySegment().get(LAYOUT, alignedStart() + longNo * ALIGNMENT);
    }

    default long fullLongNo(int longNo) {
        return memorySegment().get(LAYOUT, firstAlignedStart() + longNo * ALIGNMENT);
    }

    @SuppressWarnings("unused")
    default long longAt(int longIndex) {
        return memorySegment().get(UNALIGNED_LAYOUT, alignedStart() + longIndex);
    }

    default long tail() {
        long endIndex = endIndex();
        if (startIndex() < ALIGNMENT) {
            long tail = endIndex % ALIGNMENT;
            return LineSegments.bytesAt(memorySegment(), alignedEnd(), tail);
        }
        long tail = endIndex % ALIGNMENT;
        long value = memorySegment().get(UNALIGNED_LAYOUT, endIndex - ALIGNMENT);
        long shift = (ALIGNMENT - tail) * ALIGNMENT;
        return value >> shift;
    }

    default long slowTail() {
        long endIndex = endIndex();
        long tail = endIndex % ALIGNMENT;
        return LineSegments.bytesAt(memorySegment(), alignedEnd(), tail);
    }

    default int tailLength() {
        return Math.toIntExact(endIndex() % ALIGNMENT);
    }

    default byte byteAt(long i) {
        return memorySegment().get(JAVA_BYTE, startIndex() + i);
    }

    default long bytesAt(long offset, long count) {
        return LineSegments.bytesAt(memorySegment(), startIndex() + offset, count);
    }

    default LineSegment slice(long startIndex, long endIndex) {
        return LineSegments.of(memorySegment(), startIndex, endIndex);
    }

    long ALIGNMENT = LineSegments.ALIGNMENT;
}
