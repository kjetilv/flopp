package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.segments.ImmutableLineSegment;
import com.github.kjetilv.flopp.kernel.util.Bits;

import java.lang.foreign.MemorySegment;
import java.nio.charset.Charset;
import java.util.function.LongSupplier;
import java.util.stream.LongStream;

import static com.github.kjetilv.flopp.kernel.MemorySegments.*;
import static com.github.kjetilv.flopp.kernel.segments.HashedLineSegment.*;
import static java.lang.foreign.ValueLayout.*;

@SuppressWarnings("unused")
public interface LineSegment extends Range, Comparable<LineSegment> {

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
        long startIndex = startIndex();
        long endIndex = endIndex();
        long len = endIndex - startIndex;
        if (len == 0L) {
            return 0L;
        }
        int headStart = (int) (startIndex % ALIGNMENT_INT);
        int headLen = (ALIGNMENT_INT - headStart) % ALIGNMENT_INT;
        int tailLen = (int) endIndex % ALIGNMENT_INT;

        return (endIndex - startIndex - (headLen + tailLen) >> ALIGNMENT_POW) +
               (headLen == 0 ? 0 : 1) +
               (tailLen == 0 ? 0 : 1);
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

    default LineSegment unalignedHashedWith(int hash) {
        return unalignedHash(hash, this);
    }

    default LineSegment alignedHashedWith(int hash) {
        return alignedHash(hash, this);
    }

    default LineSegment hashedWith(int hash, boolean aligned) {
        return hash(hash, this, aligned);
    }

    default boolean matches(LineSegment other) {
        return this.length() == other.length() && MemorySegment.mismatch(
            memorySegment(), startIndex(), endIndex(),
            other.memorySegment(), other.startIndex(), other.endIndex()
        ) == -1;
    }

    @SuppressWarnings("unused")
    default LineSegment immutable() {
        return this instanceof Immutable
            ? this
            : new ImmutableLineSegment(memorySegment(), startIndex(), endIndex());
    }

    @SuppressWarnings("unused")
    default LineSegment copy() {
        MemorySegment buffer = MemorySegments.createAligned(length());
        copyBytes(memorySegment(), startIndex(), buffer, length());
        return new ImmutableLineSegment(buffer, 0, length());
    }

    @SuppressWarnings("unused")
    default LineSegment copyTo(LineSegment receiver, long offset) {
        long length = length();
        MemorySegments.copyBytes(
            this.memorySegment(), startIndex(),
            receiver.memorySegment(), offset, length
        );
        return LineSegments.of(receiver.memorySegment(), offset, offset + length);
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
        long headLen = headLength();
        long tailLen = tailLength();
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

    default long headLength() {
        long head = startIndex() % ALIGNMENT_INT;
        return head == 0L ? 0 : ALIGNMENT_INT - (int) head;
    }

    default long head(long head) {
        long offset = memorySegment().get(JAVA_LONG, alignedStart());
        return offset >>> head * ALIGNMENT_INT;
    }

    default long head() {
        long startIndex = startIndex();
        long endIndex = endIndex();
        long length = endIndex - startIndex;
        long headOffset = startIndex % ALIGNMENT_INT;
        long shift = headOffset * ALIGNMENT_INT;
        return memorySegment().get(JAVA_LONG, startIndex - headOffset) >>> shift;
    }

    default long tailLength() {
        return endIndex() % ALIGNMENT_INT;
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

    default int unaligedIntAt(long i) {
        return memorySegment().get(JAVA_INT_UNALIGNED, startIndex() + i);
    }

    default short unalignedShortAt(long i) {
        return memorySegment().get(JAVA_SHORT_UNALIGNED, startIndex() + i);
    }

    default long bytesAt(long offset, long count) {
        return MemorySegments.bytesAt(memorySegment(), startIndex() + offset, count);
    }

    default LineSegment slice(long startIndex, long endIndex) {
        long base = this.startIndex();
        return LineSegments.of(memorySegment(), base + startIndex, base + endIndex);
    }

    default LineSegment prefix(int b) {
        long index = indexOf(b);
        return index < 0 ? this : slice(0, index);
    }

    default long indexOf(int b) {
        LongSupplier longSupplier = longSupplier();
        long count = shiftedLongsCount();
        long index = 0;
        for (long l = 0; l < count; l++) {
            long data = longSupplier.getAsLong();
            int longIndex = Bits.indexOf(b, data);
            if (longIndex >= 0) {
                return index + longIndex;
            }
            index += ALIGNMENT_INT;
        }
        return -1;
    }

    @SuppressWarnings("NullableProblems")
    @Override
    default int compareTo(LineSegment o) {
        return LineSegments.compare(this, o);
    }

    MemorySegment memorySegment();

    interface Immutable {
    }

    interface Hashed {
    }
}
