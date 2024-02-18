package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Shape;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public final class LineSegments {

    public static String toString(LineSegment line) {
        return toString(line, Math.toIntExact(line.length()));
    }

    public static String toString(LineSegment line, int len) {
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = line.byteAt(i);
        }
        return new String(bytes);
    }

    static byte[] toBytes(LineSegment line) {
        return slice(line).toArray(ValueLayout.JAVA_BYTE);
    }

    static BitwisePartitionHandler.Mediator actionMediator(Partition partition, Shape shape) {
        return PartitionActionMediator.create(partition, shape);
    }

    static long bytesAt(MemorySegment memorySegment, long offset, long count) {
        long l = 0;
        for (long i = count - 1; i >= 0; i--) {
            byte b = memorySegment.get(JAVA_BYTE, offset + i);
            l = (l << Bits.ALIGNMENT) + b;
        }
        return l;
    }

    private LineSegments() {
    }

    static final long[] CLEAR_HEAD = {
        0x0000000000000000L,
        0x00000000000000FFL,
        0x000000000000FFFFL,
        0x0000000000FFFFFFL,
        0x00000000FFFFFFFFL,
        0x000000FFFFFFFFFFL,
        0x0000FFFFFFFFFFFFL,
        0x00FFFFFFFFFFFFFFL,
    };

    private static MemorySegment slice(LineSegment line) {
        return line.memorySegment().asSlice(line.startIndex(), line.endIndex());
    }
}
