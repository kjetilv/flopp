package com.github.kjetilv.flopp.kernel.bits;

import java.lang.foreign.MemorySegment;

@SuppressWarnings("PackageVisibleField")
public final class MutableLine implements MemorySegments.LineSegment {

    int partitionNo;

    MemorySegment memorySegment;

    long offset;

    long length;

    @Override
    public MemorySegment memorySegment() {
        return memorySegment;
    }

    @Override
    public long offset() {
        return offset;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{offset()}+\{length()}]";
    }
}
