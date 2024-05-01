package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.Bits;

import java.lang.foreign.MemorySegment;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static java.lang.foreign.ValueLayout.*;

public final class LineSegments {

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
            int longs = Math.toIntExact(segment.alignedCount());
            int firstLong = headLen == 0 ? 0 : 1;
            int tailLen = segment.tailLength();
            int offset = headLen;
            long alignedStart = segment.alignedStart();
            for (int i = firstLong; i < longs; i++) {
                long data = segment.memorySegment().get(JAVA_LONG, alignedStart + i * ALIGNMENT);
                Bits.transferDataTo(data, offset, string);
                offset += 8;
            }
            if (tailLen > 0) {
                if (length < ALIGNMENT) {
                    long data = segment.tail();
                    Bits.transferDataTo(data, offset, tailLen, string);
                } else {
                    long data = segment.memorySegment().get(JAVA_LONG_UNALIGNED, segment.endIndex() - ALIGNMENT);
                    long shift = ALIGNMENT * (ALIGNMENT - tailLen);
                    data >>= shift;
                    Bits.transferDataTo(data, offset, tailLen, string);
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
        if (count < ALIGNMENT) {
            return readHead(memorySegment, offset, count);
        }
        long bytes = 0;
        for (long i = count - 1; i >= 0; i--) {
            byte b = memorySegment.get(JAVA_BYTE, offset + i);
            bytes = (bytes << ALIGNMENT) + (b & 0xFFL);
        }
        return bytes;
    }

    public static long readHead(MemorySegment memorySegment, long offset, long length) {
        return switch (Math.toIntExact(length)) {
            case 0 -> 0L;
            case 1 -> memorySegment.get(JAVA_BYTE, offset);
            case 2 -> memorySegment.get(JAVA_SHORT_UNALIGNED, offset);
            case 3 -> (long) memorySegment.get(JAVA_SHORT_UNALIGNED, offset) +
                      ((long)memorySegment.get(JAVA_BYTE, offset + 2) << 16L);
            case 4 -> (long) memorySegment.get(JAVA_INT_UNALIGNED, offset);
            case 5 -> (long) memorySegment.get(JAVA_INT_UNALIGNED, offset) +
                      ((long) memorySegment.get(JAVA_BYTE, offset + 4) << 32L);
            case 6 -> (long) memorySegment.get(JAVA_INT_UNALIGNED, offset) +
                      ((long) memorySegment.get(JAVA_SHORT_UNALIGNED, offset + 4) << 32L);
            case 7 -> (long) memorySegment.get(JAVA_INT_UNALIGNED, offset) +
                      ((long) memorySegment.get(JAVA_SHORT_UNALIGNED, offset + 4) << 32L) +
                      ((long) memorySegment.get(JAVA_BYTE, offset + 6) << 48L);
            default -> throw new IllegalStateException("Invalid head: " + length);
        };
    }

    public static long readTail(MemorySegment memorySegment, long limit, int length) {
        return switch (length) {
            case 0 -> 0L;
            case 1 -> memorySegment.get(JAVA_BYTE, limit - 1);
            case 2 -> memorySegment.get(JAVA_SHORT_UNALIGNED, limit - 2);
            case 3 -> ((long) memorySegment.get(JAVA_SHORT_UNALIGNED, limit - 2) << 8L) +
                      (memorySegment.get(JAVA_BYTE, limit - 3) & 0xFF);
            case 4 -> memorySegment.get(JAVA_INT_UNALIGNED, limit - 4);
            case 5 -> ((long) memorySegment.get(JAVA_INT_UNALIGNED, limit - 4) << 8L) +
                      (memorySegment.get(JAVA_BYTE, limit - 5) & 0xFF);
            case 6 -> ((long) memorySegment.get(JAVA_INT_UNALIGNED, limit - 4) << 16L) +
                      (memorySegment.get(JAVA_SHORT_UNALIGNED, limit - 6) & 0xFFFF);
            case 7 -> ((long) memorySegment.get(JAVA_INT_UNALIGNED, limit - 4) << 24L) +
                      (memorySegment.get(JAVA_SHORT_UNALIGNED, limit - 6) << 8 & 0xFFFFFF) +
                      (memorySegment.get(JAVA_BYTE, limit - 7) & 0xFF);
            default -> throw new IllegalStateException("Invalid tail: " + length);
        };
    }

    private LineSegments() {
    }

    public static final long ALIGNMENT = 8L;

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
