package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.Bits;

import java.util.Objects;
import java.util.Spliterators;
import java.util.function.LongConsumer;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT_POW;
import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

final class LineSegmentAlignedLongSpliterator extends Spliterators.AbstractLongSpliterator {

    private final int length;

    private final int headLen;

    private final LineSegment segment;

    private final long alignedStart;

    private final long alignedEnd;

    LineSegmentAlignedLongSpliterator(LineSegment segment, int length) {
        super(length >> ALIGNMENT_POW + 2, IMMUTABLE | ORDERED);
        this.segment = Objects.requireNonNull(segment, "segment");
        this.headLen = segment.headLength();
        this.alignedStart = segment.alignedStart();
        this.alignedEnd = segment.alignedEnd();
        this.length = length;
    }

    @Override
    public boolean tryAdvance(LongConsumer action) {
        if (headLen > 0) {
            long data = segment.head() << ALIGNMENT * (ALIGNMENT - headLen);
            action.accept(data);
        }
        if (length > headLen) {
            long startPosition = alignedStart + (headLen == 0 ? 0 : ALIGNMENT);
            for (long pos = startPosition; pos < alignedEnd; pos += ALIGNMENT) {
                long data = segment.memorySegment().get(JAVA_LONG, pos);
                action.accept(data);
            }
            if (segment.endIndex() % ALIGNMENT > 0L) {
                long data = segment.tail();
                long truncated = Bits.truncate(data, segment.tailLength());
                action.accept(truncated);
            }
        }
        return false;
    }
}
