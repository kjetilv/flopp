package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.LongConsumer;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

final class LineSegmentAlignedLongSpliterator extends Spliterators.AbstractLongSpliterator {

    private final int length;

    private final int headLen;

    private final LineSegment segment;

    private final long alignedStart;

    private final long alignedEnd;

    LineSegmentAlignedLongSpliterator(LineSegment segment, int length) {
        super(length / ALIGNMENT + 2, IMMUTABLE | ORDERED);
        this.segment = Objects.requireNonNull(segment, "segment");
        this.headLen = segment.headLength();
        this.alignedStart = segment.alignedStart();
        this.alignedEnd = segment.alignedEnd();
        this.length = length;
    }

    @Override
    public boolean tryAdvance(LongConsumer action) {
        if (headLen > 0) {
            long data = segment.head();
            long shifted = data << ALIGNMENT * (ALIGNMENT - headLen);
            action.accept(shifted);
        }
        if (length > headLen) {
            long endIndex = segment.endIndex();
            MemorySegment memorySegment = segment.memorySegment();
            long startPosition = alignedStart + (headLen == 0 ? 0 : ALIGNMENT);
            for (long pos = startPosition; pos < alignedEnd; pos += ALIGNMENT) {
                long data = memorySegment.get(JAVA_LONG, pos);
                action.accept(data);
            }
            int tailLen = (int)(endIndex % ALIGNMENT);
            if (tailLen > 0) {
                long data = segment.tail();
                action.accept(data);
            }
        }
        return false;
    }
}
