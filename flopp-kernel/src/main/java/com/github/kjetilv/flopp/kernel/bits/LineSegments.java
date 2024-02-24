package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Shape;

import java.lang.foreign.MemorySegment;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static com.github.kjetilv.flopp.kernel.bits.Bits.ALIGNMENT;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public final class LineSegments {

    public static String asString(LineSegment line) {
        return asString(line, Math.toIntExact(line.length()));
    }

    public static String asString(LineSegment line, int len) {
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = line.byteAt(i);
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
        return new ImmutableSliceLine(memorySegment);
    }

    public static LineSegment of(MemorySegment memorySegment, long start, long end) {
        return new ImmutableLine(memorySegment, start, end);
    }

    public static LineSegment ofRange(MemorySegment memorySegment, long startIndex, long endIndex) {
        return new ImmutableLine(memorySegment, startIndex, endIndex);
    }

    public static String asString(MemorySegment segment, long start, long end) {
        return of(segment, start, end).asString();
    }

    public static LineSegment of(long l) {
        return of(l, Math.toIntExact(ALIGNMENT));
    }

    public static LineSegment of(long l, int len) {
        return of(new String(
            new byte[] {
                (byte) (l & 0xFF),
                (byte) (l >> 8L & 0xFF),
                (byte) (l >> 16L & 0xFF),
                (byte) (l >> 24L & 0xFF),
                (byte) (l >> 32L & 0xFF),
                (byte) (l >> 40L & 0xFF),
                (byte) (l >> 48L & 0xFF),
                (byte) (l >> 56L & 0xFF)
            },
            0,
            len
        ));
    }

    static BitwisePartitionHandler.Mediator actionMediator(Partition partition, Shape shape) {
        return PartitionActionMediator.create(partition, shape);
    }

    static long bytesAt(MemorySegment memorySegment, long offset, long count) {
        long l = 0;
        for (long i = count - 1; i >= 0; i--) {
            byte b = memorySegment.get(JAVA_BYTE, offset + i);
            l = (l << ALIGNMENT) + (b & 0xFFL);
        }
        return l;
    }

    private LineSegments() {
    }

}
