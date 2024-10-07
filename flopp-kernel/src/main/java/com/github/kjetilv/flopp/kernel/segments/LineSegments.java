package com.github.kjetilv.flopp.kernel.segments;

import com.github.kjetilv.flopp.kernel.Range;
import com.github.kjetilv.flopp.kernel.bits.Bits;

import java.lang.foreign.MemorySegment;
import java.nio.charset.Charset;
import java.util.function.LongSupplier;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import static com.github.kjetilv.flopp.kernel.segments.MemorySegments.fromEdgeLong;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

@SuppressWarnings({"DuplicatedCode", "unused"})
public final class LineSegments {

    public static int hashCode(LineSegment segment) {
        LineSegmentTraverser.Reusable reusable = LineSegmentTraverser.create(segment);
        return hashCode(reusable, reusable.size());
    }

    public static int hashCode(LongSupplier supplier, long count) {
        int hashCode = 17;
        for (int i = 0; i < count; i++) {
            long next = supplier.getAsLong();
            hashCode = nextHash(hashCode, next);
        }
        return hashCode;
    }

    public static int nextHash(int hash, long next) {
        return hash * 31 + Long.hashCode(next);
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

    public static LineSegment cat(LineSegment... segments) {
        long length = 0;
        for (LineSegment segment : segments) {
            length += segment.length();
        }
        MemorySegment memorySegment = MemorySegments.ofLength(length);
        long offset = 0;
        for (LineSegment segment : segments) {
            long l = segment.length();
            MemorySegments.copyBytes(segment.memorySegment(), memorySegment, offset, l);
            offset += l;
        }
        return LineSegments.of(memorySegment, length);
    }

    public static LongSupplier alignedLongSupplier(LineSegment segment) {
        return longSupplier(segment, true);
    }

    public static LongSupplier shiftedLongSupplier(LineSegment segment) {
        return longSupplier(segment, false);
    }

    public static LongSupplier longSupplier(LineSegment segment, boolean align) {
        return LineSegmentTraverser.create(segment, align);
    }

    public static LongStream longs(LineSegment segment, boolean align) {
        long length = align ? segment.alignedLongsCount() : segment.shiftedLongsCount();
        if (length == 0) {
            return LongStream.empty();
        }
        return StreamSupport.longStream(
            new SuppliedLongSpliterator(longSupplier(segment, align), length),
            false
        );
    }

    public static long[] asLongs(LineSegment segment) {
        return asLongs(segment, null);
    }

    public static long[] asLongs(LineSegment segment, long[] buffer) {
        LineSegmentTraverser.Reusable reusable = LineSegmentTraverser.create(segment);
        long[] ls = buffer == null ? new long[(int) reusable.size()] : buffer;
        reusable.forEach((i, l) -> ls[i] = l);
        return ls;
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

    public static LineSegment of(MemorySegment memorySegment, long length) {
        return of(memorySegment, 0L, length);
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

    public final static LineSegment NIL = new ImmutableLineSegment(
        MemorySegment.NULL,
        0L,
        0L
    );

    public static final int HASH_PRIME = 31;

    private static final LongSupplier EMPTY_LONG_SUPPLIER = () -> 0x0L;

    private static final long[] NO_LONGS = new long[0];

    private static final byte[] NO_BYTES = new byte[0];

    static LineSegment segmentOfSize(long size) {
        return of(MemorySegments.createAligned(size), 0, size);
    }
}
