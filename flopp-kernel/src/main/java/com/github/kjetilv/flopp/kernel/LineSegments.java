package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.Bits;
import com.github.kjetilv.flopp.kernel.bits.MemorySegments;

import java.lang.foreign.MemorySegment;
import java.nio.charset.Charset;
import java.util.function.LongSupplier;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT;
import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT_INT;
import static java.lang.foreign.ValueLayout.*;

@SuppressWarnings({"DuplicatedCode", "unused"})
public final class LineSegments {

    public static boolean equals(LineSegment seg1, LineSegment seg2) {
        if (seg1 == null || seg2 == null) {
            return (seg1 == null) == (seg2 == null);
        }
        if (seg1.length() != seg2.length()) {
            return false;
        }
        LongSupplier sup1 = alignedLongSupplier(seg1);
        LongSupplier sup2 = alignedLongSupplier(seg2);
        for (long l = 0; l < seg1.alignedLongsCount(); l++) {
            if (sup1.getAsLong() != sup2.getAsLong()) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("UnnecessaryParentheses")
    public static int hashCode(LineSegment segment) {
        int length = Math.toIntExact(segment.length());
        if (length == 0L) {
            return 0;
        }
        long hashCode = 0L;
        int headLen = segment.headLength();
        long alignedStart = segment.alignedStart();
        long alignedEnd = segment.alignedEnd();
        long endIndex = segment.endIndex();
        long tailLen = endIndex % ALIGNMENT;
        long longs = segment.fullLongCount() + (headLen > 0 ? 1 : 0) + (tailLen > 0 ? 1 : 0);
        if (headLen > 0) {
            long data = segment.head();
            hashCode = data * 31L ^ (longs == 0 ? 1 : longs);
        }
        if (length > ALIGNMENT) {
            MemorySegment memorySegment = segment.memorySegment();
            long startPosition = alignedStart + (headLen == 0 ? 0 : ALIGNMENT_INT);
            for (long pos = startPosition; pos < alignedEnd; pos += ALIGNMENT_INT) {
                long data = memorySegment.get(JAVA_LONG, pos);
                hashCode += (data * 31L) ^ (longs - (pos / ALIGNMENT_INT));
            }
            if (tailLen > 0) {
                hashCode += readTail(segment, memorySegment, length, endIndex, tailLen, false) * 31L;
            }
        }
        return (int) hashCode;
    }

    @SuppressWarnings("ConstantValue")
    public static int compare(LineSegment segment1, LineSegment segment2) {
        LongSupplier longSupplier1 = longSupplier(segment1, true);
        LongSupplier longSupplier2 = longSupplier(segment2, true);
        long length1 = segment1.shiftedLongsCount();
        long length2 = segment2.shiftedLongsCount();
        long length = Math.min(length1, length2);
        for (int i = 0; i < length; i++) {
            long l1 = longSupplier1.getAsLong();
            long l2 = longSupplier2.getAsLong();
            if (l1 < l2) {
                return -1;
            }
            if (l1 > l2) {
                return 1;
            }
        }
        return length1 < length2 ? -1
            : length2 > length1 ? 1
                : 0;
    }

    public static LongSupplier alignedLongSupplier(LineSegment segment) {
        int length = Math.toIntExact(segment.length());
        return length == 0
            ? () -> 0x0L
            : new LineSegmentAlignedLongSupplier(segment, length);
    }

    public static LongSupplier longSupplier(LineSegment segment, boolean shift) {
        int length = Math.toIntExact(segment.length());
        if (length == 0) {
            return () -> 0x0L;
        }
        int headLen = segment.headLength();
        return headLen > 0 && shift
            ? new LineSegmentShiftedLongSupplier(segment, length, headLen)
            : alignedLongSupplier(segment);
    }

    public static LongStream longs(LineSegment segment, boolean shift) {
        return shift ? shiftedLongs(segment) : alignedLongs(segment);
    }

    public static LongStream alignedLongs(LineSegment segment) {
        int length = Math.toIntExact(segment.length());
        return length == 0
            ? LongStream.empty()
            : StreamSupport.longStream(new LineSegmentAlignedLongSpliterator(segment, length), false);
    }

    public static LongStream shiftedLongs(LineSegment segment) {
        int length = Math.toIntExact(segment.length());
        if (length == 0) {
            return LongStream.empty();
        }
        int headLen = segment.headLength();
        if (ALIGNMENT - headLen + length < ALIGNMENT_INT) {
            long data = segment.head();
            return LongStream.of(data);
        }
        return headLen == 0
            ? alignedLongs(segment)
            : StreamSupport.longStream(
                new LineSegmentShiftedLongSpliterator(segment, length, headLen),
                false
            );
    }

    public static String asString(LineSegment segment, Charset charset) {
        long startIndex = segment.startIndex();
        long endIndex = segment.endIndex();
        MemorySegment memorySegment = segment.memorySegment();
        return MemorySegments.fromEdgeLong(
            memorySegment,
            startIndex,
            endIndex,
            null,
            charset
        );
    }

    public static String asString(LineSegment segment, byte[] buffer, Charset charset) {
        long startIndex = segment.startIndex();
        long endIndex = segment.endIndex();
        MemorySegment memorySegment = segment.memorySegment();
        return MemorySegments.fromEdgeLong(
            memorySegment,
            startIndex,
            endIndex,
            buffer,
            charset
        );
    }

    public static String asBoundedString(LineSegment segment, byte[] buffer, Charset charset) {
        long startIndex = segment.startIndex();
        long endIndex = segment.endIndex();
        MemorySegment memorySegment = segment.memorySegment();
        return MemorySegments.fromLongsWithinBounds(
            memorySegment,
            startIndex,
            endIndex,
            buffer,
            charset
        );
    }

    public static byte[] simpleBytes(LineSegment segment) {
        int length = Math.toIntExact(segment.length());
        if (length == 0) {
            return NO_BYTES;
        }
        byte[] bytes = new byte[length];
        MemorySegment.copy(
            segment.memorySegment(),
            JAVA_BYTE,
            segment.startIndex(),
            bytes,
            0,
            length
        );
        return bytes;
    }

    public static String fromLongBytes(LineSegment segment, Charset charset) {
        return asString(segment, charset);
    }

    public static byte[] asBytes(LineSegment segment) {
        int length = Math.toIntExact(segment.length());
        if (length == 0) {
            return NO_BYTES;
        }
        byte[] string = new byte[length];
        int headLen = segment.headLength();
        if (headLen > 0) {
            long data = segment.head();
            Bits.transferLimitedDataTo(data, 0, Math.min(length, headLen), string);
        }
        if (length > headLen) {
            long alignedStart = segment.alignedStart();
            long longs = (segment.alignedEnd() - alignedStart) / ALIGNMENT_INT;
            int firstLong = headLen == 0 ? 0 : 1;
            long endIndex = segment.endIndex();
            int tailLen = Math.toIntExact(endIndex % ALIGNMENT);
            MemorySegment memorySegment = segment.memorySegment();
            int transferOffset = headLen;
            long position = alignedStart + firstLong * ALIGNMENT;
            for (int i = firstLong; i < longs; i++) {
                long data = memorySegment.get(JAVA_LONG, position);
                Bits.transferDataTo(data, transferOffset, string);
                transferOffset += ALIGNMENT_INT;
                position += ALIGNMENT;
            }
            if (tailLen > 0) {
                long data = readTail(segment, memorySegment, length, endIndex, tailLen, false);
                Bits.transferLimitedDataTo(data, transferOffset, tailLen, string);
            }
        }
        return string;
    }

    public static String asString(LineSegment segment, int len, Charset charset) {
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = segment.byteAt(i);
        }
        return new String(bytes, charset);
    }

    public static LineSegment of(String string, Charset charset) {
        return of(string.getBytes(charset));
    }

    public static LineSegment of(byte[] bytes) {
        return of(MemorySegments.of(bytes), 0, bytes.length);
    }

    public static LineSegment of(long[] ls, int length) {
        return of(MemorySegment.ofArray(ls), 0, length);
    }

    public static LineSegment of(MemorySegment memorySegment, long start, long end) {
        return new ImmutableLineSegment(memorySegment, start, end);
    }

    public static LineSegment of(long l) {
        return of(Bits.toBytes(l));
    }

    public static String toString(LineSegment lineSegment) {
        return lineSegment.getClass().getSimpleName() + "[" +
               lineSegment.startIndex() + "-" + lineSegment.endIndex() +
               "]";
    }

    public static String asString(MemorySegment segment, long start, long end, Charset charset) {
        return asString(of(segment, start, end), null, charset);
    }

    static long readTail(
        LineSegment segment,
        MemorySegment memorySegment,
        int length,
        long endIndex,
        long tailLen,
        boolean truncate
    ) {
        if (length < ALIGNMENT_INT) {
            return segment.tail(truncate);
        }
        long data = memorySegment.get(JAVA_LONG_UNALIGNED, endIndex - ALIGNMENT_INT);
        long shift = ALIGNMENT_INT * (ALIGNMENT_INT - tailLen);
        return data >> shift;
    }

    private LineSegments() {
    }

    public static final byte[] NO_BYTES = new byte[0];
}
