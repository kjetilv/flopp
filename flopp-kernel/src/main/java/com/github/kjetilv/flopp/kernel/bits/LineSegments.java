package com.github.kjetilv.flopp.kernel.bits;

import java.lang.foreign.ValueLayout;

public final class LineSegments {

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
