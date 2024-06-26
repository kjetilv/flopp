package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;

import java.lang.foreign.MemorySegment;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT;
import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

public final class BitwiseLongSupplier implements Function<LineSegment, BitwiseLongSupplier.Reusable> {

    public static Reusable create() {
        return create(null, false);
    }

    public static Reusable create(boolean align) {
        return create(null, align);
    }

    public static Reusable create(LineSegment segment) {
        return create(segment, false);
    }

    public static Reusable create(LineSegment segment, boolean align) {
        return new BitwiseLongSupplier(align).apply(segment);
    }

    private final boolean align;

    private final ReusableBase none = new Null();

    private final ReusableBase aligned = new Aligned();

    private final ReusableBase shifted;

    public BitwiseLongSupplier(boolean align) {
        this.align = align;
        this.shifted = this.align ? null : new Shifted();
    }

    @Override
    public Reusable apply(LineSegment segment) {
        if (segment == null) {
            return none;
        }
        int headLen = segment.headLength();
        ReusableBase reusable = headLen == 0 || align ? aligned : shifted;
        return reusable.initialize(segment, headLen);
    }

    private abstract sealed class ReusableBase implements Reusable {

        LineSegment segment;

        MemorySegment memorySegment;

        int headLen;

        long position;

        int tailLen;

        @Override
        public final Reusable apply(LineSegment lineSegment) {
            return BitwiseLongSupplier.this.apply(lineSegment);
        }

        abstract Reusable initialize(LineSegment segment, int headLen);
    }

    private final class Null extends ReusableBase {

        @Override
        public long getAsLong() {
            return 0x0L;
        }

        @Override
        public long size() {
            return 0L;
        }

        @Override
        Reusable initialize(LineSegment segment, int headLen) {
            return this;
        }
    }

    private final class Shifted extends ReusableBase {

        private long headStart;

        private long alignedEnd;

        private long endIndex;

        private long headShift;

        private int length;

        private int tailShift;

        private long data;

        @Override
        public ReusableBase initialize(LineSegment segment, int headLen) {
            this.segment = segment;
            this.memorySegment = segment.memorySegment();
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
        public long size() {
            return segment.shiftedLongsCount();
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

    private final class Aligned extends ReusableBase {

        private long alignedStart;

        private long alignedEnd;

        @Override
        public ReusableBase initialize(LineSegment segment, int headLen) {
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
        public long size() {
            return segment.alignedLongsCount();
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

    public interface Reusable extends LongSupplier, Function<LineSegment, Reusable> {

        long size();
    }
}
