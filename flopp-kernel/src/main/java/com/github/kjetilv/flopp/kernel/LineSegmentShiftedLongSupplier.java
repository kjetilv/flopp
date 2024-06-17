package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.Bits;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.function.LongSupplier;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT;
import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

final class LineSegmentShiftedLongSupplier implements LongSupplier {

    private final LineSegment segment;

    private final int headLen;

    private final int length;

    private final long endIndex;

    private final int tailLen;

    private final int headShift;

    private final int tailShift;

    private final long alignedEnd;

    private long position;

    private long data;

    private final MemorySegment memorySegment;

    private final long headStart;

    LineSegmentShiftedLongSupplier(LineSegment segment, int length, int headLen) {
        this.segment = Objects.requireNonNull(segment, "segment");
        this.headStart = this.segment.headStart();
        this.memorySegment = this.segment.memorySegment();
        this.endIndex = this.segment.endIndex();

        this.length = length;

        this.headLen = headLen;
        this.headShift = (int)(headLen * ALIGNMENT);

        this.tailLen = (int)(endIndex % ALIGNMENT);
        this.tailShift = (int)((ALIGNMENT - headLen) * ALIGNMENT);

        this.alignedEnd = this.segment.alignedEnd();
        this.position = this.segment.alignedStart() + (headLen > 0 ? ALIGNMENT : 0);

        this.data = this.segment.head();
    }

    @Override
    public long getAsLong() {
        if (headStart + length < ALIGNMENT) {
            return Bits.truncate(data, length);
        }
        if (position == endIndex) {
            return data;
        }
        if (position < alignedEnd) {
            long alignedData = memorySegment.get(JAVA_LONG, position);
            try {
                long shifted = alignedData << headShift;
                data |= shifted;
                return data;
            } finally {
                data = alignedData >>> tailShift;
                position += ALIGNMENT;
            }
        }
        if (position == this.alignedEnd && tailLen > 0) {
            long alignedData = segment.tail();
            try {
                long shifted = alignedData << headShift;
                data |= shifted;
                return data;
            } finally {
                position += ALIGNMENT_INT;
            }
        }
        int headStart = ALIGNMENT_INT - headLen;
        if (tailLen > headStart) {
            int restTail = tailLen - headStart;
            if (restTail > 0) {
                long alignedData = segment.tail();
                long remainingData = alignedData >> headStart * ALIGNMENT;
                return remainingData;
            }
        }
        return 0x0L;
    }
}
