package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.function.LongSupplier;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

final class LineSegmentShiftedLongSupplier implements LongSupplier {

    private final LineSegment segment;

    private final int length;

    private final long endIndex;

    private final int tailLen;

    private final int headShift;

    private final int tailShift;

    private final long alignedEnd;

    private long position;

    private long data;

    private final MemorySegment memorySegment;

    LineSegmentShiftedLongSupplier(LineSegment segment, int length, int headLen) {
        this.segment = Objects.requireNonNull(segment, "segment");
        this.memorySegment = this.segment.memorySegment();
        this.length = length;
        this.endIndex = this.segment.endIndex();

        this.tailLen = Math.toIntExact(endIndex % ALIGNMENT);
        this.headShift = Math.toIntExact(headLen * ALIGNMENT);
        this.tailShift = Math.toIntExact((ALIGNMENT - headLen) * ALIGNMENT);

        this.alignedEnd = this.segment.alignedEnd();
        this.data = this.segment.head(true);
        this.position = this.segment.alignedStart() + (headLen > 0 ? ALIGNMENT : 0);
    }

    @Override
    public long getAsLong() {
        if (length < ALIGNMENT) {
            return data;
        }
        if (position < alignedEnd) {
            long alignedData = memorySegment.get(JAVA_LONG, position);
            try {
                data |= alignedData << headShift;
                return data;
            } finally {
                data = alignedData >> tailShift;
                position += ALIGNMENT;
            }
        }
        if (position == this.alignedEnd && tailLen > 0) {
            try {
                long alignedData = LineSegments.readTail(segment, memorySegment, length, endIndex, tailLen, true);
                data |= alignedData << headShift;
                return data;
            } finally {
                position += ALIGNMENT;
            }
        }
        return 0x0L;
    }
}
