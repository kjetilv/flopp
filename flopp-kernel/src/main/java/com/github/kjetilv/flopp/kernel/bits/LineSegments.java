package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Shape;

import java.lang.foreign.MemorySegment;

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

}
