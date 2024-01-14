package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;

@SuppressWarnings("PackageVisibleField")
final class MutableLine implements MemorySegments.LineSegment {

    int partitionNo;

    long lineNo;

    MemorySegment memorySegment;

    long offset;

    int length;

    @Override
    public int partitionNo() {
        return partitionNo;
    }

    @Override
    public long lineNo() {
        return lineNo;
    }

    @Override
    public MemorySegment memorySegment() {
        return memorySegment;
    }

    @Override
    public long offset() {
        return offset;
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{lineNo}/\{partitionNo}: \{offset}-\{length}]";
    }
}
