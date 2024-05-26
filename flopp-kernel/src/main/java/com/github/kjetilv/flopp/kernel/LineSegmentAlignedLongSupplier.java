package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.function.LongSupplier;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

final class LineSegmentAlignedLongSupplier implements LongSupplier {

    private final long alignedStart;

    private final int headLen;

    private final long endIndex;

    private final int tailLen;

    private final LineSegment segment;

    private final int length;

    private long position;

    private final MemorySegment memorySegment;

    private final long alignedEnd;

    LineSegmentAlignedLongSupplier(LineSegment segment, int length) {
        this.segment = Objects.requireNonNull(segment, "segment");
        this.memorySegment = segment.memorySegment();
        this.length = length;
        this.alignedStart = this.segment.alignedStart();
        this.alignedEnd = this.segment.alignedEnd();
        this.headLen = this.segment.headLength();
        this.endIndex = this.segment.endIndex();
        this.tailLen = Math.toIntExact(this.endIndex % ALIGNMENT);

        this.position = this.alignedStart;
    }

    @Override
    public long getAsLong() {
        if (position == alignedStart && headLen > 0) {
            try {
                long data = segment.head(true);
                long shifted = data << ALIGNMENT * (ALIGNMENT - headLen);
                return shifted;
            } finally {
                position += ALIGNMENT;
            }
        }
        if (position < alignedEnd) {
            try {
                long data = memorySegment.get(JAVA_LONG, position);
                return data;
            } finally {
                position += ALIGNMENT;
            }
        }
        if (position == alignedEnd && tailLen > 0) {
            try {
                long data = LineSegments.readTail(segment, memorySegment, length, endIndex, tailLen, true);
                return data;
            } finally {
                position += ALIGNMENT;
            }
        }
        return 0x0L;
    }
}
