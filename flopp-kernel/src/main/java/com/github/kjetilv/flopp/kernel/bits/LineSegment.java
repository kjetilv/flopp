package com.github.kjetilv.flopp.kernel.bits;

import java.lang.foreign.MemorySegment;

@SuppressWarnings("unused")
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

    default String tooString() {
        return LineSegments.toString(this);
    }
}
