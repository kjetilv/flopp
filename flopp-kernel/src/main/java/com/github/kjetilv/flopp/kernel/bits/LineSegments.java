package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Shape;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

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

    private LineSegments() {
    }

    private static MemorySegment slice(LineSegment line) {
        return line.memorySegment()
            .asSlice(line.offset(), line.length());
    }
}
