package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.Bits;

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

    default long underlyingSize() {
        return memorySegment().byteSize();
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
     *
     * @return Aligned start
     */
    default long alignedStart() {
        return startIndex() - headStart();
    }

    /**
     * The aligned end of this segment. May be ahead of/less-than {@link #endIndex() end index}.
     *
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
        return startsOnEdge()
            ? LineSegments.bytesAt(memorySegment(), startIndex(), Math.min(headLength(), length()))
            : memorySegment().get(UNALIGNED_LAYOUT, startIndex());
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
        return tail(false);
    }

    default long tail(boolean truncate) {
        if (endsOnEdge()) {
            return LineSegments.bytesAt(memorySegment(), alignedEnd(), tailLength());
        }
        long value = memorySegment().get(UNALIGNED_LAYOUT, alignedEnd());
        return truncate
            ? Bits.lowerBytes(value, tailLength())
            : value;
    }

    default int tailLength() {
        return Math.toIntExact(endIndex() % ALIGNMENT);
    }

    default boolean startsOnEdge() {
        return underlyingSize() - startIndex() < ALIGNMENT;
    }

    default boolean endsOnEdge() {
        return underlyingSize() - endIndex() < ALIGNMENT;
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
