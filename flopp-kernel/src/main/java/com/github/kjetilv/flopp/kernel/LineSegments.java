package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.Bits;

import java.lang.foreign.MemorySegment;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static java.lang.foreign.ValueLayout.*;

public final class LineSegments {

    public static String asString(LineSegment segment) {
        int len = Math.toIntExact(segment.length());
        byte[] string = new byte[len];
        int headLen = segment.headLength();
        if (headLen > 0) {
            long data = segment.head();
            Bits.transferDataTo(data, 0, Math.min(len, headLen), string);
        }
        if (len > headLen) {
            int longs = Math.toIntExact(segment.alignedCount());
            int firstLong = headLen == 0 ? 0 : 1;
            int tailLen = segment.tailLength();
            int offset = headLen;
            for (int i = firstLong; i < longs; i++) {
                long data = segment.longNo(i);
                Bits.transferDataTo(data, offset, string);
                offset += 8;
            }
            if (tailLen > 0) {
                long data = segment.tail();
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
        return new ImmutableSlice(memorySegment);
    }

    public static LineSegment of(MemorySegment memorySegment, long start, long end) {
        return new Immutable(memorySegment, start, end);
    }

    public static String asString(MemorySegment segment, long start, long end) {
        return of(segment, start, end).asString();
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

    public static long bytesAt(MemorySegment memorySegment, long offset, long count) {
        long bytes = 0;
        for (long i = count - 1; i >= 0; i--) {
            byte b = memorySegment.get(JAVA_BYTE, offset + i);
            bytes = (bytes << ALIGNMENT) + (b & 0xFFL);
        }
        return bytes;
    }

    public static long readHead(MemorySegment memorySegment, int length, long first) {
        return switch (length) {
            case 1 -> memorySegment.get(JAVA_BYTE, first);
            case 2 -> memorySegment.get(JAVA_SHORT_UNALIGNED, first);
            case 3 -> (long) memorySegment.get(JAVA_SHORT_UNALIGNED, first) +
                      (memorySegment.get(JAVA_BYTE, first + 2) << 16);
            case 4 -> (long) memorySegment.get(JAVA_INT_UNALIGNED, first);
            case 5 -> (long) memorySegment.get(JAVA_INT_UNALIGNED, first) +
                      ((long) memorySegment.get(JAVA_BYTE, first + 4) << 32);
            case 6 -> (long) memorySegment.get(JAVA_INT_UNALIGNED, first) +
                      (long) memorySegment.get(JAVA_SHORT_UNALIGNED, first + 4) << 32;
            case 7 -> (long) memorySegment.get(JAVA_INT_UNALIGNED, first) +
                      ((long) memorySegment.get(JAVA_SHORT_UNALIGNED, first + 4) << 32) +
                      (long) memorySegment.get(JAVA_BYTE, first + 6) >> 48;
            default -> throw new IllegalStateException("Invalid head: " + length);
        };
    }

    public static long readTail(MemorySegment memorySegment, int length, long last) {
        return switch (length) {
            case 1 -> memorySegment.get(JAVA_BYTE, last - 1);
            case 2 -> memorySegment.get(JAVA_SHORT_UNALIGNED, last - 2);
            case 3 -> ((long) memorySegment.get(JAVA_SHORT_UNALIGNED, last - 2) << 8) +
                      memorySegment.get(JAVA_BYTE, last - 3);
            case 4 -> memorySegment.get(JAVA_INT_UNALIGNED, last - 4);
            case 5 -> ((long) memorySegment.get(JAVA_INT_UNALIGNED, last - 4) << 8) +
                      memorySegment.get(JAVA_BYTE, last - 5);
            case 6 -> ((long) memorySegment.get(JAVA_INT_UNALIGNED, last - 4) << 16) +
                      memorySegment.get(JAVA_SHORT_UNALIGNED, last - 6);
            case 7 -> ((long) memorySegment.get(JAVA_INT_UNALIGNED, last - 4) << 24) +
                      ((long) memorySegment.get(JAVA_SHORT_UNALIGNED, last - 6) << 8) +
                      memorySegment.get(JAVA_BYTE, last - 7);
            default -> throw new IllegalStateException("Invalid tail: " + length);
        };
    }

    private LineSegments() {
    }

    public static final long ALIGNMENT = JAVA_LONG.byteSize();

    record ImmutableSlice(MemorySegment memorySegment, long length)
        implements LineSegment {

        ImmutableSlice(MemorySegment memorySegment) {
            this(memorySegment, memorySegment.byteSize());
        }

        @Override
        public long startIndex() {
            return 0;
        }

        @Override
        public long endIndex() {
            return length;
        }

        @Override
        public LineSegment immutable() {
            return this;
        }

        @Override
        public LineSegment immutableSlice() {
            return this;
        }

        @Override
        public String toString() {
            return LineSegments.toString(this);
        }
    }

    record Immutable(MemorySegment memorySegment, long startIndex, long endIndex)
        implements LineSegment {

        @Override
        public LineSegment immutable() {
            return this;
        }

        @Override
        public String toString() {
            return LineSegments.toString(this);
        }
    }
}
