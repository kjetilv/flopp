package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.MemorySegments;

import java.lang.foreign.MemorySegment;
import java.util.Objects;

import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;

public final class ReusableLineSegment implements LineSegment {

    private final MemorySegment segment;

    private long length;

    public ReusableLineSegment(int length) {
        this(MemorySegments.ofLength(length));
    }

    public ReusableLineSegment(MemorySegment segment) {
        this.segment = Objects.requireNonNull(segment, "segment");
        if (this.segment.isReadOnly()) {
            throw new IllegalArgumentException("Should be writable: " + segment);
        }
    }

    @Override
    public MemorySegment memorySegment() {
        return segment;
    }

    @Override
    public long startIndex() {
        return 0;
    }

    @Override
    public long endIndex() {
        return length;
    }

    @Override
    public String toString() {
        return LineSegments.toString(this);
    }

    public void setLong(int pos, long data) {
        segment.setAtIndex(JAVA_LONG_UNALIGNED, pos, data);
    }

    public void setLength(long length) {
        this.length = length;
    }
}
