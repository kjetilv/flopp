package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.Bits;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.LongConsumer;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT_POW;
import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT;
import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

class LineSegmentShiftedLongSpliterator extends Spliterators.AbstractLongSpliterator {

    private final LineSegment segment;

    private final long alignedStart;

    private final long alignedEnd;

    private final MemorySegment memorySegment;

    private final long length;

    private final int headLen;

    private final int headShift;

    private final int tailShift;

    private final int tailLen;

    private final long endIndex;

    LineSegmentShiftedLongSpliterator(LineSegment segment, int length, int headLen) {
        super(length >> ALIGNMENT_POW + 2, IMMUTABLE | ORDERED);
        this.segment = Objects.requireNonNull(segment, "segment");
        this.memorySegment = this.segment.memorySegment();
        this.alignedStart = this.segment.alignedStart();
        this.alignedEnd = this.segment.alignedEnd();
        this.endIndex = this.segment.endIndex();
        this.length = segment.length();
        this.headLen = headLen;
        this.headShift = this.headLen * ALIGNMENT_INT;
        this.tailLen = (int)(this.endIndex % ALIGNMENT);
        this.tailShift = (ALIGNMENT_INT - headLen) * ALIGNMENT_INT;
    }

    @Override
    public boolean tryAdvance(LongConsumer action) {
        long data = segment.head();
        long position = alignedStart + ALIGNMENT;
        if (position >= endIndex) {
            action.accept(Bits.truncate(data, (int) length));
            return false;
        }
        while (position < alignedEnd) {
            long alignedData = memorySegment.get(JAVA_LONG, position);
            long shifted = alignedData << headShift;
            data |= shifted;
            action.accept(data);
            data = alignedData >>> tailShift;
            position += ALIGNMENT;
        }
        if (position == this.alignedEnd && tailLen > 0) {
            long alignedData = segment.tail();
            long shifted = alignedData << headShift;
            data |= shifted;
            action.accept(data);
            int headStart = ALIGNMENT_INT - headLen;
            int restTail = tailLen - headStart;
            if (restTail > 0) {
                long remainingData = alignedData >> headStart * ALIGNMENT;
                action.accept(remainingData);
            }
        } else if (headLen > 0) {
            action.accept(data);
        }
        return false;
    }
}
