package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.Bits;
import com.github.kjetilv.flopp.kernel.bits.MemorySegments;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.function.LongSupplier;
import java.util.stream.LongStream;

import static com.github.kjetilv.flopp.kernel.HashedLineSegment.hash;
import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.*;
import static java.lang.foreign.ValueLayout.*;

@SuppressWarnings("unused")
public interface LineSegment extends Range, Comparable<LineSegment> {

    MemorySegment memorySegment();

    default long longsCount() {
        return longsCount(false);
    }

    default long longsCount(boolean align) {
        return align ? alignedLongsCount() : shiftedLongsCount();
    }

    default long shiftedLongsCount() {
        long length = length();
        long fullLongs = length >> ALIGNMENT_POW;
        long remainder = length % ALIGNMENT_INT;
        return fullLongs + (remainder == 0 ? 0 : 1);
    }

    default long alignedLongsCount() {
        int headLen = headLength();
        int tailLen = tailLength();
        long len = length();
        if (len == 0) {
            return 0L;
        }
        if (len < ALIGNMENT_INT) {
            return (headLen > 0 ? 1 : 0) + (tailLen > 0 ? 1 : 0);
        }
        long length = endIndex() - tailLen - startIndex() - headLen >> ALIGNMENT_POW;
        return (headLen > 0 ? 1 : 0) + length + (tailLen > 0 ? 1 : 0);
    }

    default LongStream alignedLongStream() {
        return longStream(true);
    }

    default LongStream shiftedLongStream() {
        return longStream(false);
    }

    default LongStream longStream() {
        return longStream(false);
    }

    default LongStream longStream(boolean align) {
        return LineSegments.longs(this, align);
    }

    default LongSupplier alignedLongSupplier() {
        return longSupplier(true);
    }

    default LongSupplier shiftedLongSupplier() {
        return longSupplier(false);
    }

    default LongSupplier longSupplier() {
        return longSupplier(false);
    }

    default LongSupplier longSupplier(boolean align) {
        return LineSegments.longSupplier(this, align);
    }

    default LineSegment hashed() {
        return this instanceof Hashed ? this : hash(this);
    }

    default LineSegment hashedWith(int hash) {
        return hash(hash, this);
    }

    default boolean matches(LineSegment other) {
        return this.length() == other.length() && LineSegments.mismatch(this, other) < 0;
    }

    @SuppressWarnings("unused")
    default LineSegment immutable() {
        return this instanceof Immutable
            ? this
            : new ImmutableLineSegment(memorySegment(), startIndex(), endIndex());
    }

    @SuppressWarnings("unused")
    default LineSegment copy() {
        MemorySegment buffer =
            MemorySegment.ofBuffer(ByteBuffer.allocateDirect(Math.toIntExact(length())));
        copyBytes(memorySegment(), startIndex(), buffer, length());
        return new ImmutableLineSegment(buffer, 0, length());
    }

    @SuppressWarnings("unused")
    default LineSegment immutableSlice() {
        return immutableSlice(endIndex());
    }

    default LineSegment immutableSlice(long length) {
        return new ImmutableSliceSegment(
            memorySegment().asSlice(startIndex(), length),
            length
        );
    }

    default boolean hasRange(int startIndex, int endIndex) {
        return startIndex() == startIndex && endIndex() == endIndex;
    }

    default String asString() {
        return asString(Charset.defaultCharset());
    }

    default String asString(Charset charset) {
        return asString(null, charset);
    }

    default String asString(byte[] buffer) {
        return asString(buffer, Charset.defaultCharset());
    }

    default String asString(byte[] buffer, Charset charset) {
        return LineSegments.asString(this, buffer, charset);
    }

    default String asString(int length) {
        return asString(length, Charset.defaultCharset());
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
        return alignedEnd() - alignedStart() >> ALIGNMENT_POW;
    }

    @SuppressWarnings("UnnecessaryParentheses")
    default long fullLongCount() {
        int headLen = headLength();
        int tailLen = tailLength();
        return ((endIndex() - tailLen) - (startIndex() + headLen)) >> ALIGNMENT_POW;
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
        return head == 0L ? 0 : ALIGNMENT_INT - (int) head;
    }

    default long head(long head) {
        long offset = memorySegment().get(JAVA_LONG, alignedStart());
        long shift = head * ALIGNMENT_INT;
        return offset >> shift;
    }

    default long head() {
        long startIndex = startIndex();
        long endIndex = endIndex();
        long length = endIndex - startIndex;
        long headOffset = startIndex % ALIGNMENT_INT;
        long shift = headOffset * ALIGNMENT_INT;
        return memorySegment().get(JAVA_LONG, startIndex - headOffset) >> shift;
    }

    default long truncatedHead() {
        return Bits.truncate(head(), headLength());
    }

    default int tailLength() {
        return Math.toIntExact(endIndex() % ALIGNMENT_INT);
    }

    default long tail() {
        MemorySegment ms = memorySegment();
        long endIndex = endIndex();
        return MemorySegments.tail(ms, endIndex);
    }

    default long longNo(long longNo) {
        return longAt(alignedStart() + longNo * ALIGNMENT_INT);
    }

    default long fullLongNo(long longNo) {
        return longAt(firstAlignedStart() + longNo * ALIGNMENT);
    }

    default long longAt(long offset) {
        return memorySegment().get(JAVA_LONG, offset);
    }

    default long unalignedLongNo(long longNo) {
        return memorySegment().get(JAVA_LONG_UNALIGNED, startIndex() + longNo * ALIGNMENT);
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

    interface Immutable {
    }

    interface Hashed {
    }
}
