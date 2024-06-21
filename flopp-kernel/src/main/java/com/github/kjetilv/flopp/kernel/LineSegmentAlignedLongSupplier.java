package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.function.LongSupplier;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

final class LineSegmentAlignedLongSupplier implements LongSupplier {

    private final long alignedStart;

    private final int headLen;

    private final int tailLen;

    private final LineSegment segment;

    private long position;

    private final MemorySegment memorySegment;

    private final long alignedEnd;

    LineSegmentAlignedLongSupplier(LineSegment segment) {
        this.segment = Objects.requireNonNull(segment, "segment");
        this.memorySegment = segment.memorySegment();
        this.alignedStart = this.segment.alignedStart();
        this.alignedEnd = this.segment.alignedEnd();
        this.headLen = this.segment.headLength();
        long endIndex = this.segment.endIndex();
        this.tailLen = Math.toIntExact(endIndex % ALIGNMENT);

        this.position = this.alignedStart;
    }

    @Override
    public long getAsLong() {
        try {
            if (position == alignedStart && headLen > 0) {
                return segment.head() << ALIGNMENT * (ALIGNMENT - headLen);
            }
            if (position < alignedEnd) {
                return memorySegment.get(JAVA_LONG, position);
            }
            if (position == alignedEnd && tailLen > 0) {
                return segment.tail();
            }
            return 0x0L;
        } finally {
            position += ALIGNMENT;
        }
    }
}
