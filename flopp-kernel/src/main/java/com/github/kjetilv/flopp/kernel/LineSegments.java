package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.ValueLayout;

public final class LineSegments {

    public static Mediator<LineSegment> mediator(Partition partition, Shape shape) {
        return PartitionMediator.create(partition, shape, LineSegment::immutable);
    }

    public static String toString(LineSegment line) {
        return new String(toBytes(line));
    }

    public static byte[] toBytes(LineSegment line) {
        return line.memorySegment()
            .asSlice(line.offset(), line.length())
            .toArray(ValueLayout.JAVA_BYTE);

    }

    private LineSegments() {
    }
}
