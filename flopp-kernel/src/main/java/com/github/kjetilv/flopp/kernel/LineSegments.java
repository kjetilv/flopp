package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.Bits;

import java.lang.foreign.MemorySegment;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;

public final class LineSegments {

    public static int hashCode(LineSegment lineSegment) {
        return 0;
    }

    public static String asString(LineSegment segment) {
        int length = Math.toIntExact(segment.length());
        if (length == 0) {
            return "";
        }
        byte[] string = new byte[length];
        int headLen = segment.headLength();
        if (headLen > 0) {
            long data = segment.head();
            Bits.transferDataTo(data, 0, Math.min(length, headLen), string);
        }
        if (length > headLen) {
            long alignedStart = segment.alignedStart();
            long longs = (segment.alignedEnd() - alignedStart) / ALIGNMENT;
            int firstLong = headLen == 0 ? 0 : 1;
            long endIndex = segment.endIndex();
            long tailLen = endIndex % ALIGNMENT;
            int offset = headLen;
            for (int i = firstLong; i < longs; i++) {
                long data = segment.memorySegment().get(JAVA_LONG, alignedStart + i * ALIGNMENT);
                Bits.transferDataTo(data, offset, string);
                offset += 8;
            }
            if (tailLen > 0) {
                if (length < ALIGNMENT) {
                    long data = segment.tail(false);
                    Bits.transferDataTo(data, offset, Math.toIntExact(tailLen), string);
                } else {
                    long data = segment.memorySegment().get(JAVA_LONG_UNALIGNED, endIndex - ALIGNMENT);
                    long shift = ALIGNMENT * (ALIGNMENT - tailLen);
                    data >>= shift;
                    Bits.transferDataTo(data, offset, Math.toIntExact(tailLen), string);
                }
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
}
