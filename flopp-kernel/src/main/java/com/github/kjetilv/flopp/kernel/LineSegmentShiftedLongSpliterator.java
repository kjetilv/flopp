package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.LongConsumer;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT;
import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

class LineSegmentShiftedLongSpliterator extends Spliterators.AbstractLongSpliterator {

    private final int length;

    private final LineSegment segment;

    private final long alignedStart;

    private final long alignedEnd;

    private final MemorySegment memorySegment;

    private final int headShift;

    private final int tailShift;

    private final long tailLen;

    private final long endIndex;

    LineSegmentShiftedLongSpliterator(LineSegment segment, int length, int headLen) {
        super(length / ALIGNMENT + 2, IMMUTABLE | ORDERED);
        this.segment = Objects.requireNonNull(segment, "segment");
        this.memorySegment = this.segment.memorySegment();
        this.length = length;
        this.tailShift = (ALIGNMENT_INT - headLen) * ALIGNMENT_INT;
        this.headShift = headLen * ALIGNMENT_INT;
        this.alignedStart = this.segment.alignedStart();
        this.alignedEnd = this.segment.alignedEnd();
        this.endIndex = this.segment.endIndex();
        this.tailLen = this.endIndex % ALIGNMENT;
    }

    @Override
    public boolean tryAdvance(LongConsumer action) {
        long data = segment.head(true);
        long position = alignedStart + ALIGNMENT;
        while (position < alignedEnd) {
            try {
                long alignedData = memorySegment.get(JAVA_LONG, position);
                data |= alignedData << headShift;
                action.accept(data);
                data = alignedData >> tailShift;
            } finally {
                position += ALIGNMENT;
            }
        }
        if (tailLen > 0) {
            long alignedData = LineSegments.readTail(segment, memorySegment, length, endIndex, tailLen, true);
            data |= alignedData << headShift;
            action.accept(data);
        }
        return false;
    }
}
