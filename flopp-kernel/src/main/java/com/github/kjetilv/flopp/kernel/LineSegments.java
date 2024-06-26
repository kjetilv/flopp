package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.Bits;
import com.github.kjetilv.flopp.kernel.bits.BitwiseLongSpliterator;
import com.github.kjetilv.flopp.kernel.bits.BitwiseLongSupplier;
import com.github.kjetilv.flopp.kernel.bits.MemorySegments;

import java.lang.foreign.MemorySegment;
import java.nio.charset.Charset;
import java.util.function.LongSupplier;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.*;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

@SuppressWarnings({"DuplicatedCode", "unused"})
public final class LineSegments {

    public static boolean equals(LineSegment seg1, LineSegment seg2) {
        if (seg1 == null || seg2 == null) {
            return (seg1 == null) == (seg2 == null);
        }
        long count1 = seg1.longsCount();
        long count2 = seg2.longsCount();
        if (count1 != count2) {
            return false;
        }
        LongSupplier sup1 = seg1.longSupplier();
        LongSupplier sup2 = seg2.longSupplier();
        for (long l = 0; l < count1; l++) {
            if (sup1.getAsLong() != sup2.getAsLong()) {
                return false;
            }
        }
        return true;
    }

    public static long hashCode(LineSegment segment) {
        return hashCode(segment, BitwiseLongSupplier.create(segment));
    }

    public  static long hashCode(
        LineSegment segment,
        BitwiseLongSupplier.Reusable reusable
    ) {
        long count = segment.shiftedLongsCount();
        long hashCode = 0L;
        for (int i = 0; i < count; i++) {
            long next = reusable.getAsLong();
            hashCode += (next * 31L) ^ (count - i);
        }
        return hashCode;
    }

    @SuppressWarnings("ConstantValue")
    public static int compare(LineSegment segment1, LineSegment segment2) {
        LongSupplier longSupplier1 = segment1.longSupplier(false);
        LongSupplier longSupplier2 = segment2.longSupplier(false);
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
        return longSupplier(segment, true);
    }

    public static LongSupplier shiftedLongSupplier(LineSegment segment) {
        return longSupplier(segment, false);
    }

    public static LongSupplier longSupplier(LineSegment segment, boolean align) {
        return BitwiseLongSupplier.create(segment, align);
    }

    public static LongStream longs(LineSegment segment, boolean align) {
        long length = align ? segment.alignedLongsCount() : segment.shiftedLongsCount();
        if (length == 0) {
            return LongStream.empty();
        }
        LongSupplier supplier = longSupplier(segment, align);
        return StreamSupport.longStream(new BitwiseLongSpliterator(length, supplier), false);
    }

    public static long[] asLongs(LineSegment segment) {
        return asLongs(segment, null);
    }

    public static long[] asLongs(LineSegment segment, long[] buffer) {
        int length = (int) segment.length();
        if (length == 0) {
            return NO_LONGS;
        }
        int headLen = segment.headLength();
        long[] longs = buffer == null
            ? new long[(int) segment.shiftedLongsCount()]
            : buffer;
        return headLen == 0
            ? asAlignedLongs(segment, longs, length)
            : asShiftedLongs(segment, longs, length, headLen);
    }

    public static String asString(LineSegment segment) {
        return asString(segment, Charset.defaultCharset());
    }

    public static String asString(LineSegment segment, Charset charset) {
        long startIndex = segment.startIndex();
        long endIndex = segment.endIndex();
        MemorySegment memorySegment = segment.memorySegment();
        return fromEdgeLong(memorySegment, startIndex, endIndex, null, charset);
    }

    public static String asString(LineSegment segment, byte[] buffer) {
        return asString(segment, buffer, Charset.defaultCharset());
    }

    public static String asString(LineSegment segment, byte[] buffer, Charset charset) {
        long startIndex = segment.startIndex();
        long endIndex = segment.endIndex();
        MemorySegment memorySegment = segment.memorySegment();
        return fromEdgeLong(memorySegment, startIndex, endIndex, buffer, charset);
    }

    public static String asBoundedString(LineSegment segment, byte[] buffer) {
        return asBoundedString(segment, buffer, Charset.defaultCharset());
    }

    public static String asBoundedString(LineSegment segment, byte[] buffer, Charset charset) {
        long startIndex = segment.startIndex();
        long endIndex = segment.endIndex();
        MemorySegment memorySegment = segment.memorySegment();
        return MemorySegments.fromLongsWithinBounds(memorySegment, startIndex, endIndex, buffer, charset);
    }

    public static byte[] simpleBytes(LineSegment segment) {
        int length = (int) segment.length();
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

    public static String fromLongBytes(LineSegment segment) {
        return fromLongBytes(segment, Charset.defaultCharset());
    }

    public static String fromLongBytes(LineSegment segment, Charset charset) {
        return asString(segment, charset);
    }

    public static byte[] asBytes(LineSegment segment) {
        int length = (int) segment.length();
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
            long longs = segment.alignedEnd() - alignedStart >> ALIGNMENT_POW;
            int firstLong = headLen == 0 ? 0 : 1;
            long endIndex = segment.endIndex();
            int tailLen = (int) (endIndex % ALIGNMENT);
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
                long data = segment.tail();
                Bits.transferLimitedDataTo(data, transferOffset, tailLen, string);
            }
        }
        return string;
    }

    public static String asString(LineSegment segment, int len) {
        return asString(segment, len, Charset.defaultCharset());
    }

    public static String asString(LineSegment segment, int len, Charset charset) {
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = segment.byteAt(i);
        }
        return new String(bytes, charset);
    }

    public static LineSegment of(String string) {
        return of(string, Charset.defaultCharset());
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

    public static LineSegment of(MemorySegment memorySegment, Range range) {
        return of(memorySegment, range.startIndex(), range.endIndex());
    }

    public static LineSegment of(MemorySegment memorySegment, long start, long end) {
        return new ImmutableLineSegment(memorySegment, start, end);
    }

    public static LineSegment of(long l) {
        return of(Bits.toBytes(l));
    }

    public static String toString(LineSegment lineSegment) {
        return LineSegment.class.getSimpleName() + "[" +
               lineSegment.startIndex() + "-" + lineSegment.endIndex() +
               "]";
    }

    public static String asString(MemorySegment segment, long start, long end) {
        return asString(segment, start, end, Charset.defaultCharset());
    }

    public static String asString(MemorySegment segment, long start, long end, Charset charset) {
        return asString(of(segment, start, end), null, charset);
    }

    public static String fromLongsWithinBounds(LineSegment lineSegment, byte[] target) {
        return fromLongsWithinBounds(lineSegment, target, Charset.defaultCharset());
    }

    public static String fromLongsWithinBounds(LineSegment lineSegment, byte[] target, Charset charset) {
        return MemorySegments.fromLongsWithinBounds(
            lineSegment.memorySegment(),
            lineSegment.startIndex(),
            lineSegment.endIndex(),
            target,
            charset
        );
    }

    private LineSegments() {
    }

    private static final LongSupplier EMPTY_LONG_SUPPLIER = () -> 0x0L;

    private static final long[] NO_LONGS = new long[0];

    private static final byte[] NO_BYTES = new byte[0];

    private static long[] asAlignedLongs(LineSegment segment, long[] buffer, int length) {
        long alignedEnd = segment.alignedEnd();
        long startPosition = segment.alignedStart();
        int index = 0;
        for (long pos = startPosition; pos < alignedEnd; pos += ALIGNMENT) {
            long data = segment.memorySegment().get(JAVA_LONG, pos);
            buffer[index++] = data;
        }
        if (segment.endIndex() % ALIGNMENT > 0L) {
            long data = segment.tail();
            long truncated = Bits.truncate(data, segment.tailLength());
            buffer[index] = truncated;
        }
        return buffer;
    }

    private static long[] asShiftedLongs(LineSegment segment, long[] buffer, int length, int headLen) {
        MemorySegment memorySegment = segment.memorySegment();
        long endIndex = segment.endIndex();

        int headShift = (int) (headLen * ALIGNMENT);

        int tailLen = (int) (endIndex % ALIGNMENT);
        int tailShift = (int) ((ALIGNMENT - headLen) * ALIGNMENT);

        long alignedEnd = segment.alignedEnd();
        long position = segment.alignedStart() + (headLen > 0 ? ALIGNMENT : 0);

        long data = segment.head();
        int index = 0;
        if (headLen >= length) {
            buffer[index] = Bits.truncate(data, length);
            return buffer;
        }
        while (position < alignedEnd) {
            long alignedData = memorySegment.get(JAVA_LONG, position);
            long shifted = alignedData << headShift;
            data |= shifted;
            buffer[index++] = data;
            data = alignedData >>> tailShift;
            position += ALIGNMENT_INT;
        }
        if (position == alignedEnd && tailLen > 0) {
            long alignedData = segment.tail();
            long shifted = alignedData << headShift;
            data |= shifted;
            buffer[index++] = data;
            int headStart = ALIGNMENT_INT - headLen;
            int restTail = tailLen - headStart;
            if (restTail > 0) {
                long remainingData = alignedData >> headStart * ALIGNMENT;
                buffer[index] = remainingData;
            }
        } else if (headLen > 0) {
            buffer[index] = data;
        }
        return buffer;
    }
}
