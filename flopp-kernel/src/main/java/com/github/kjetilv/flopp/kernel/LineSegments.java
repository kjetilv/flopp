package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public final class LineSegments {

    public static String asString(LineSegment line) {
        return asString(line, Math.toIntExact(line.length()));
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
        return STR."\{ls.getClass().getSimpleName()}[\{ls.startIndex()}-\{ls.endIndex()}]";
    }

    public static long bytesAt(MemorySegment memorySegment, long offset, long count) {
        long bytes = 0;
        for (long i = count - 1; i >= 0; i--) {
            byte b = memorySegment.get(JAVA_BYTE, offset + i);
            bytes = (bytes << ALIGNMENT) + (b & 0xFFL);
        }
        return bytes;
    }

    private LineSegments() {
    }

    public static final ValueLayout.OfLong LAYOUT = ValueLayout.JAVA_LONG;

    public static final long ALIGNMENT = ValueLayout.JAVA_LONG.byteSize();

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
