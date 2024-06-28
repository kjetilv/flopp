package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;

import java.lang.foreign.MemorySegment;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT;
import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

public abstract sealed class BitwiseTraverser
    implements Function<LineSegment, BitwiseTraverser.Reusable> {

    public static Reusable create() {
        return new MultiModeSuppler().blank();
    }

    public static Reusable create(boolean align) {
        return (align ? new AlignedTraverser() : new MultiModeSuppler()).blank();
    }

    public static Reusable create(LineSegment segment) {
        return new MultiModeSuppler().apply(segment);
    }

    public static Reusable create(LineSegment segment, boolean align) {
        return (align ? new AlignedTraverser() : new MultiModeSuppler()).apply(segment);
    }

    @SuppressWarnings("PackageVisibleField")
    final ReusableBase aligned = new Aligned();

    @SuppressWarnings("PackageVisibleField")
    final ReusableBase shifted = new Shifted();

    private final ReusableBase none = new Null();

    public Reusable blank() {
        return none;
    }

    private static final class AlignedTraverser extends BitwiseTraverser {

        @Override
        public Reusable apply(LineSegment segment) {
            int headLen = segment.headLength();
            return aligned.initialize(segment, headLen);
        }
    }

    private static final class MultiModeSuppler extends BitwiseTraverser {

        @Override
        public Reusable apply(LineSegment segment) {
            int headLen = segment.headLength();
            return (headLen == 0 ? aligned : shifted).initialize(segment, headLen);
        }
    }

    private abstract sealed class ReusableBase implements Reusable {

        @Override
        public final Reusable apply(LineSegment lineSegment) {
            return BitwiseTraverser.this.apply(lineSegment);
        }

        abstract Reusable initialize(LineSegment segment, int headLen);
    }

    private final class Null extends ReusableBase {

        @Override
        public long getAsLong() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void forEach(IndexedLongConsumer consumer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() {
            throw new UnsupportedOperationException();
        }

        @Override
        Reusable initialize(LineSegment segment, int headLen) {
            throw new UnsupportedOperationException();
        }
    }

    private final class Shifted extends ReusableBase {

        private LineSegment segment;

        private MemorySegment memorySegment;

        private int headLen;

        private long position;

        private int tailLen;

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
        public void forEach(IndexedLongConsumer consumer) {
            if (length == 0) {
                return;
            }
            long data = segment.head();
            if (headLen >= length) {
                consumer.accept(0, Bits.truncate(data, length));
                return;
            }
            int index = 0;
            while (position < alignedEnd) {
                long alignedData = memorySegment.get(JAVA_LONG, position);
                long shifted = alignedData << headShift;
                data |= shifted;
                consumer.accept(index++, data);
                data = alignedData >>> tailShift;
                position += ALIGNMENT_INT;
            }
            if (position == alignedEnd && tailLen > 0) {
                long alignedData = segment.tail();
                long shifted = alignedData << headShift;
                data |= shifted;
                consumer.accept(index++, data);
                int headStart = ALIGNMENT_INT - headLen;
                int restTail = tailLen - headStart;
                if (restTail > 0) {
                    long remainingData = alignedData >> headStart * ALIGNMENT;
                    consumer.accept(index, remainingData);
                }
            } else if (headLen > 0) {
                consumer.accept(index, data);
            }
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

        private LineSegment segment;

        private MemorySegment memorySegment;

        private int headLen;

        private long position;

        private int tailLen;

        private long alignedStart;

        private long alignedEnd;

        private long length;

        @Override
        public ReusableBase initialize(LineSegment segment, int headLen) {
            this.segment = segment;
            this.memorySegment = segment.memorySegment();
            this.length = this.segment.length();
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
        public void forEach(IndexedLongConsumer consumer) {
            if (length == 0) {
                return;
            }
            int index = 0;
            for (long pos = alignedStart; pos < alignedEnd; pos += ALIGNMENT) {
                long data = segment.memorySegment().get(JAVA_LONG, pos);
                consumer.accept(index++, data);
            }
            if (segment.endIndex() % ALIGNMENT > 0L) {
                long data = segment.tail();
                long truncated = Bits.truncate(data, segment.tailLength());
                consumer.accept(index, truncated);
            }
        }

        @Override
        public long getAsLong() {
            long next = position == alignedStart && headLen > 0 ? segment.head() << ALIGNMENT * (ALIGNMENT - headLen)
                : position < alignedEnd ? memorySegment.get(JAVA_LONG, position)
                    : position == alignedEnd && tailLen > 0 ? segment.tail()
                        : 0x0L;
            position += ALIGNMENT;
            return next;
        }
    }

    public interface Reusable extends LongSupplier, Function<LineSegment, Reusable> {

        long size();

        void forEach(IndexedLongConsumer consumer);
    }

    @FunctionalInterface
    public interface IndexedLongConsumer extends LongConsumer {

        default void accept(long value) {
            accept(-1, value);
        }

         void accept(int index, long value);
    }
}
