package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.Bits;

import java.lang.foreign.MemorySegment;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Spliterators;
import java.util.function.LongConsumer;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.ORDERED;

@SuppressWarnings("DuplicatedCode")
public final class LineSegments {

    public static boolean equals(LineSegment seg1, LineSegment seg2) {
        if (seg1 == null || seg2 == null) {
            return (seg1 == null) == (seg2 == null);
        }
        if (seg1.length() != seg2.length()) {
            return false;
        }
        return seg1.asString().equals(seg2.asString());
    }

    public static int hashCode(LineSegment segment) {
        int length = Math.toIntExact(segment.length());
        if (length == 0L) {
            return 0;
        }
        long hashCode = 0L;
        int headLen = segment.headLength();
        long alignedStart = segment.alignedStart();
        long endIndex = segment.endIndex();
        long tailLen = endIndex % ALIGNMENT;
        long longs = segment.fullLongCount() + (headLen > 0 ? 1 : 0) + (tailLen > 0 ? 1 : 0);
        if (headLen > 0) {
            long data = segment.head(true);
            hashCode = data * 31L ^ (longs == 0 ? 1 : longs);
        }
        if (length > headLen) {
            for (int i = headLen == 0 ? 0 : 1; i < longs; i++) {
                long data = segment.memorySegment().get(JAVA_LONG, alignedStart + i * ALIGNMENT);
                hashCode += data * 31L ^ longs - i;
            }
            if (tailLen > 0) {
                hashCode += readTail(segment, length, endIndex, tailLen, false) * 31L;
            }
        }
        return (int) hashCode;
    }

    public static LongStream longs(LineSegment segment) {
        int length = Math.toIntExact(segment.length());
        if (length == 0) {
            return LongStream.empty();
        }
        int headLen = segment.headLength();
        long alignedStart = segment.alignedStart();
        long longs = (segment.alignedEnd() - alignedStart) / ALIGNMENT;
        return StreamSupport.longStream(new Spliterators.AbstractLongSpliterator(
            length / ALIGNMENT + 2,
            IMMUTABLE | ORDERED
        ) {

            @Override
            public boolean tryAdvance(LongConsumer action) {
                if (headLen > 0) {
                    action.accept(segment.head(true));
                }
                if (length > headLen) {
                    long endIndex = segment.endIndex();
                    for (int i = headLen == 0 ? 0 : 1; i < longs; i++) {
                        action.accept(segment.memorySegment().get(JAVA_LONG, alignedStart + i * ALIGNMENT));
                    }
                    int tailLen = Math.toIntExact(endIndex % ALIGNMENT);
                    if (tailLen > 0) {
                        action.accept(readTail(segment, length, endIndex, tailLen, true));
                    }
                }
                return false;
            }
        }, false);
    }

    public static LongStream longs(LineSegment segment, boolean align) {
        if (!align) {
            return longs(segment);
        }
        int length = Math.toIntExact(segment.length());
        if (length == 0) {
            return LongStream.empty();
        }
        int headLen = segment.headLength();
        if (headLen == 0) {
            return longs(segment);
        }
        int tailShift = Math.toIntExact((ALIGNMENT - headLen) * ALIGNMENT);
        int headShift = Math.toIntExact(headLen * ALIGNMENT);
        long alignedStart = segment.alignedStart();
        long longs = (segment.alignedEnd() - alignedStart) / ALIGNMENT;
        long endIndex = segment.endIndex();
        int tailLen = Math.toIntExact(endIndex % ALIGNMENT);
        if (length > headLen) {
            return StreamSupport.longStream(new Spliterators.AbstractLongSpliterator(
                length / ALIGNMENT + 2,
                IMMUTABLE | ORDERED
            ) {

                @Override
                public boolean tryAdvance(LongConsumer action) {
                    long data = segment.head(true);
                    for (int i = 1; i < longs; i++) {
                        long alignedData = segment.memorySegment().get(JAVA_LONG, alignedStart + i * ALIGNMENT);
                        data |= alignedData << headShift;
                        action.accept(data);
                        data = alignedData >> tailShift;
                    }
                    if (tailLen > 0) {
                        long alignedData = readTail(segment, length, endIndex, tailLen, true);
                        data |= alignedData << headShift;
                        action.accept(data);
                    }
                    return false;
                }
            }, false);
        }
        return LongStream.of(segment.head(true));
    }

    public static String asString(LineSegment segment) {
        int length = Math.toIntExact(segment.length());
        if (length == 0) {
            return "";
        }
        byte[] string = new byte[length];
        int headLen = segment.headLength();
        if (headLen > 0) {
            long data = segment.head(false);
            Bits.transferDataTo(data, 0, Math.min(length, headLen), string);
        }
        if (length > headLen) {
            long alignedStart = segment.alignedStart();
            long longs = (segment.alignedEnd() - alignedStart) / ALIGNMENT;
            int firstLong = headLen == 0 ? 0 : 1;
            long endIndex = segment.endIndex();
            int tailLen = Math.toIntExact(endIndex % ALIGNMENT);
            int offset = headLen;
            for (int i = firstLong; i < longs; i++) {
                long data = segment.memorySegment().get(JAVA_LONG, alignedStart + i * ALIGNMENT);
                Bits.transferDataTo(data, offset, string);
                offset += 8;
            }
            if (tailLen > 0) {
                long data = readTail(segment, length, endIndex, tailLen, false);
                Bits.transferDataTo(data, offset, tailLen, string);
            }
        }
        return new String(string, StandardCharsets.UTF_8);
    }

    public static String asString(LineSegment segment, int len) {
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = segment.byteAt(i);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static LineSegment of(String string) {
        return of(string, null);
    }

    public static LineSegment of(String string, Charset charset) {
        return of(string.getBytes(charset == null ? StandardCharsets.UTF_8 : charset));
    }

    public static LineSegment of(byte[] bytes) {
        return of(MemorySegments.of(bytes));
    }

    public static LineSegment of(MemorySegment memorySegment) {
        return new ImmutableSliceSegment(memorySegment);
    }

    public static LineSegment of(MemorySegment memorySegment, long start, long end) {
        return new ImmutableLineSegment(memorySegment, start, end);
    }

    public static LineSegment of(long l) {
        return of(l, Math.toIntExact(ALIGNMENT));
    }

    public static LineSegment of(long l, int len) {
        return of(new String(Bits.toBytes(l), 0, len));
    }

    public static String toString(LineSegment ls) {
        return ls.getClass().getSimpleName() + "[" + ls.startIndex() + "-" + ls.endIndex() + "]";
    }

    public static String asString(MemorySegment segment, long start, long end) {
        return of(segment, start, end).asString();
    }

    private LineSegments() {
    }

    static final long ALIGNMENT = 8L;

    private static long readTail(
        LineSegment segment,
        int length,
        long endIndex,
        long tailLen,
        boolean truncate
    ) {
        if (length < ALIGNMENT) {
            return segment.tail(truncate);
        }
        long data = segment.memorySegment().get(JAVA_LONG_UNALIGNED, endIndex - ALIGNMENT);
        long shift = ALIGNMENT * (ALIGNMENT - tailLen);
        return data >> shift;
    }
}
