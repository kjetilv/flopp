package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class LineSegments {

    public static Mediator<LineSegment> mediator(Partition partition, Shape shape) {
        return PartitionMediator.create(partition, shape, LineSegment::immutable);
    }

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

    public static byte[] toBytes(LineSegment line) {
        return slice(line).toArray(ValueLayout.JAVA_BYTE);

    }

    private static MemorySegment slice(LineSegment line) {
        return line.memorySegment()
            .asSlice(line.offset(), line.length());
    }

    private LineSegments() {
    }
}
