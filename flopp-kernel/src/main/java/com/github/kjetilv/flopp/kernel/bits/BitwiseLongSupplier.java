package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;

import java.lang.foreign.MemorySegment;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT;
import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

public class BitwiseLongSupplier implements Function<LineSegment, BitwiseLongSupplier.Mutable> {

    public static Mutable create() {
        return create(false);
    }

    public static Mutable create(boolean align) {
        return new BitwiseLongSupplier(align).apply(null);
    }

    private final boolean align;

    private final AbstractMutable none = new Null();

    private final AbstractMutable aligned = new Aligned();

    private final AbstractMutable shifted;

    public BitwiseLongSupplier(boolean align) {
        this.align = align;
        this.shifted = align ? null : new Shifted();
    }

    @Override
    public final Mutable apply(LineSegment segment) {
        if (segment != null) {
            int headLen = segment.headLength();
            AbstractMutable mutable = headLen == 0 || align ? aligned : shifted;
            return mutable.initialize(segment, headLen);
        }
        return none;
    }

    private abstract sealed class AbstractMutable implements Mutable {

        LineSegment segment;

        MemorySegment memorySegment;

        int headLen;

        long position;

        int tailLen;

        @Override
        public final Mutable apply(LineSegment lineSegment) {
            return BitwiseLongSupplier.this.apply(lineSegment);
        }

        abstract Mutable initialize(LineSegment segment, int headLen);
    }

    private final class Null extends AbstractMutable {

        @Override
        public long getAsLong() {
            return 0x0L;
        }

        @Override
        Mutable initialize(LineSegment segment, int headLen) {
            return this;
        }
    }

    private final class Shifted extends AbstractMutable {

        private long headStart;

        private long alignedEnd;

        private long endIndex;

        private long headShift;

        private int length;

        private int tailShift;

        private long data;

        @Override
        public AbstractMutable initialize(LineSegment segment, int headLen) {
            this.segment = segment;
            this.memorySegment = segment.
                memorySegment();
            this.headLen = headLen;
            this.headStart = this.segment.headStart();
            this.endIndex = this.segment.endIndex();

            this.length = (int) segment.length();

            this.headShift = (int) (headLen * ALIGNMENT);

            this.tailLen = (int) (endIndex % ALIGNMENT);
            this.tailShift = (int) ((ALIGNMENT - headLen) * ALIGNMENT);

            this.alignedEnd = this.segment.alignedEnd();
            this.position = this.segment.alignedStart() + (headLen > 0 ? ALIGNMENT : 0);

            this.data = this.segment.head();
            return this;
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

    private final class Aligned extends AbstractMutable {

        private long alignedStart;

        private long alignedEnd;

        @Override
        public AbstractMutable initialize(LineSegment segment, int headLen) {
            this.segment = segment;
            this.memorySegment = segment.memorySegment();
            this.headLen = headLen;
            this.alignedStart = this.segment.alignedStart();
            this.alignedEnd = this.segment.alignedEnd();
            this.headLen = this.segment.headLength();
            long endIndex = this.segment.endIndex();
            this.tailLen = Math.toIntExact(endIndex % ALIGNMENT);

            this.position = this.alignedStart;
            return this;
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

    public interface Mutable extends LongSupplier, Function<LineSegment, Mutable> {
    }
}
