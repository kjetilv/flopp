package com.github.kjetilv.flopp.kernel.bits;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class MemorySegments {

    public static String toString(LineSegment line) {
        return new String(toBytes(line));
    }

    public static byte[] toBytes(LineSegment line) {
        return line.memorySegment()
            .asSlice(line.offset(), line.length())
            .toArray(ValueLayout.JAVA_BYTE);

    }

    private MemorySegments() {
    }

    public interface LineSegment {

        MemorySegment memorySegment();

        long offset();

        long length();

        default LineSegment validate() {
            if (length() < 0) {
                throw new IllegalStateException(STR."\{this} has negative length");
            }
            return this;
        }
    }
}
