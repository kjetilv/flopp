package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.MemorySegments;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.function.LongSupplier;
import java.util.stream.LongStream;

import static java.lang.foreign.ValueLayout.*;

@SuppressWarnings("unused")
public interface LineSegment extends Range, Comparable<LineSegment> {

    MemorySegment memorySegment();

    default LineSegment aligned() {
        if (underlyingSize() % ALIGNMENT == 0) {
            return this;
        }
        throw new IllegalStateException(this + " is not aligned: " + memorySegment());
    }

    default long shiftedLongsCount() {
        long length = length();
        long fullLongs = length / ALIGNMENT;
        long remainder = length % ALIGNMENT;
        return fullLongs + (remainder == 0 ? 0 : 1);
    }

    default long alignedLongsCount() {
        int headLen = headLength();
        int tailLen = tailLength();
        long len = length();
        if (len == 0) {
            return 0L;
        }
        if (len < ALIGNMENT) {
            return (headLen > 0 ? 1 : 0) + (tailLen > 0 ? 1 : 0);
        }
        long length = (endIndex() - tailLen - (startIndex() + headLen)) / ALIGNMENT;
        return (headLen > 0 ? 1 : 0) + length + (tailLen > 0 ? 1 : 0);
    }

    default LongStream alignedLongStream() {
        return LineSegments.alignedLongs(this);
    }

    default LongSupplier alignedLongSupplier() {
        return LineSegments.alignedLongSupplier(this);
    }

    default LongStream shiftedLongStream() {
        return LineSegments.shiftedLongs(this);
    }

    default LongStream longStream(boolean shift) {
        return LineSegments.longs(this, shift);
    }

    default LongSupplier longSupplier(boolean shift) {
        return LineSegments.longSupplier(this, shift);
    }

    @SuppressWarnings("unused")
    default LineSegment immutable() {
        return new ImmutableLineSegment(memorySegment(), startIndex(), endIndex());
    }

    @SuppressWarnings("unused")
    default LineSegment copy() {
        MemorySegment buffer =
            MemorySegment.ofBuffer(ByteBuffer.allocateDirect(Math.toIntExact(length())));
        MemorySegment.copy(
            memorySegment(),
            JAVA_BYTE,
            startIndex(),
            buffer,
            JAVA_BYTE,
            0,
            length()
        );
        return new ImmutableLineSegment(buffer, 0, length());
    }

    @SuppressWarnings("unused")
    default LineSegment immutableSlice() {
        return immutableSLice(endIndex());
    }

    default LineSegment immutableSLice(long length) {
        return new ImmutableSliceSegment(
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

    default String asString(Charset charset) {
        return asString(null, charset);
    }

    default String asString(byte[] buffer, Charset charset) {
        return LineSegments.asString(this, buffer, charset);
    }

    default String asString(int length, Charset charset) {
        return LineSegments.asString(this, length, charset);
    }

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
        long tailEnd = endIndex % ALIGNMENT_INT;
        return endIndex - tailEnd;
    }

    default long alignedCount() {
        return (alignedEnd() - alignedStart()) / ALIGNMENT_INT;
    }

    @SuppressWarnings("UnnecessaryParentheses")
    default long fullLongCount() {
        int headLen = headLength();
        int tailLen = tailLength();
        return ((endIndex() - tailLen) - (startIndex() + headLen)) / ALIGNMENT_INT;
    }

    default long headStart() {
        return startIndex() % ALIGNMENT_INT;
    }

    default boolean isAlignedAtStart() {
        return startIndex() % ALIGNMENT_INT == 0L;
    }

    default boolean isAlignedAtEnd() {
        return endIndex() % ALIGNMENT_INT == 0;
    }

    default long firstAlignedStart() {
        return startIndex() + headLength();
    }

    default int headLength() {
        long head = startIndex() % ALIGNMENT_INT;
        long padding = head == 0L ? 0L : ALIGNMENT_INT - head;
        return Math.toIntExact(padding);
    }

    default long head() {
        long startIndex = startIndex();
        long endIndex = endIndex();
        long length = endIndex - startIndex;
        long headOffset = startIndex % ALIGNMENT_INT;
        long headLength = ALIGNMENT - headOffset;
        int len = (int) Math.min(headLength, length);
        if (underlyingSize() - startIndex < ALIGNMENT_INT) {
            return MemorySegments.readHead(memorySegment(), startIndex, len);
        }
        return memorySegment().get(JAVA_LONG, startIndex - headOffset) >> headOffset * ALIGNMENT;
    }

    default long head(long head) {
        long l = memorySegment().get(JAVA_LONG, alignedStart());
        return l >> head * ALIGNMENT_INT;
    }

    default long longNo(int longNo) {
        return memorySegment().get(JAVA_LONG, alignedStart() + longNo * ALIGNMENT);
    }

    default long fullLongNo(int longNo) {
        return memorySegment().get(JAVA_LONG, firstAlignedStart() + longNo * ALIGNMENT);
    }

    @SuppressWarnings("unused")
    default long longAt(int longIndex) {
        return memorySegment().get(JAVA_LONG_UNALIGNED, alignedStart() + longIndex);
    }

    default long tail() {
        return tail(false);
    }

    default long tail(boolean truncate) {
        MemorySegment ms = memorySegment();
        int tail = tailLength();
        long endIndex = endIndex();
        if (underlyingSize() - endIndex < ALIGNMENT_INT) {
            return MemorySegments.readTail(ms, endIndex, tail);
        }
        long value = ms.get(JAVA_LONG, alignedEnd());
        int shift = ALIGNMENT_INT * (ALIGNMENT_INT- tail);
        return value << shift >> shift;
    }

    default int tailLength() {
        return Math.toIntExact(endIndex() % ALIGNMENT_INT);
    }

    default byte byteAt(long i) {
        return memorySegment().get(JAVA_BYTE, startIndex() + i);
    }

    default long bytesAt(long offset, long count) {
        return MemorySegments.bytesAt(memorySegment(), startIndex() + offset, count);
    }

    default LineSegment slice(long startIndex, long endIndex) {
        return LineSegments.of(memorySegment(), startIndex, endIndex);
    }

    @Override
    default int compareTo(LineSegment o) {
        return LineSegments.compare(this, o);
    }

    long ALIGNMENT = MemorySegments.ALIGNMENT;

    int ALIGNMENT_INT = MemorySegments.ALIGNMENT_INT;
}
