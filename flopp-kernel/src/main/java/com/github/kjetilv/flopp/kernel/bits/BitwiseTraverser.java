package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.segments.LineSegment;

import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import static com.github.kjetilv.flopp.kernel.segments.LineSegments.nextHash;
import static com.github.kjetilv.flopp.kernel.io.MemorySegments.ALIGNMENT;
import static com.github.kjetilv.flopp.kernel.io.MemorySegments.ALIGNMENT_INT;

public abstract sealed class BitwiseTraverser
    implements Function<LineSegment, BitwiseTraverser.Reusable> {

    public static Reusable create() {
        return new MultiModeSuppler().blank();
    }

    public static Reusable createAligned() {
        return new AlignedTraverser().blank();
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

    @SuppressWarnings("PackageVisibleField")
    final ReusableBase empty = new Empty();

    private final ReusableBase none = new Null();

    public Reusable blank() {
        return none;
    }

    @Override
    public Reusable apply(LineSegment segment) {
        long startIndex = segment.startIndex();
        long endIndex = segment.endIndex();
        int headStart = (int) (startIndex % ALIGNMENT_INT);
        int length = (int) (endIndex - startIndex);
        if (length == 0) {
            return empty;
        }
        int headLen = headStart == 0L ? 0 : ALIGNMENT_INT - headStart;
        return baseFor(headStart).initialize(segment, headLen, headStart, startIndex, endIndex, length);
    }

    abstract ReusableBase baseFor(int headStart);

    private static final class AlignedTraverser extends BitwiseTraverser {

        @Override
        ReusableBase baseFor(int headStart) {
            return aligned;
        }
    }

    private static final class MultiModeSuppler extends BitwiseTraverser {

        @Override
        ReusableBase baseFor(int headStart) {
            return headStart == 0 ? aligned : shifted;
        }
    }

    private abstract sealed class ReusableBase implements Reusable {

        @Override
        public final Reusable apply(LineSegment lineSegment) {
            return BitwiseTraverser.this.apply(lineSegment);
        }

        protected Reusable initialize(
            LineSegment segment,
            int headLen,
            int headStart,
            long startIndex,
            long endIndex,
            int length
        ) {
            return null;
        }
    }

    private final class Null extends ReusableBase {

        @Override
        public long getAsLong() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void forEach(IndexedLongConsumer consumer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int toHashCode() {
            throw new UnsupportedOperationException();
        }
    }

    private final class Empty extends ReusableBase {

        @Override
        public long size() {
            return 0;
        }

        @Override
        public void forEach(IndexedLongConsumer consumer) {
        }

        @Override
        public long getAsLong() {
            throw new UnsupportedOperationException();
        }
    }

    private final class Shifted extends ReusableBase {

        private LineSegment segment;

        private int length;

        private int headLen;

        private int headStart;

        private int tailLen;

        private long alignedEnd;

        private long endIndex;

        private int headShift;

        private int tailShift;

        private long position;

        private long data;

        @Override
        public long size() {
            return segment.shiftedLongsCount();
        }

        @Override
        public void forEach(IndexedLongConsumer consumer) {
            if (headLen >= length) {
                consumer.accept(0, Bits.truncate(data, length));
                return;
            }
            int index = 0;
            while (position < alignedEnd) {
                long alignedData = segment.longAt(position);
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
        public int toHashCode() {
            int hash = 17;
            long data = segment.head();
            if (headLen >= length) {
                return nextHash(hash, Bits.truncate(data, length));
            }
            while (position < alignedEnd) {
                long alignedData = segment.longAt(position);
                long shifted = alignedData << headShift;
                data |= shifted;
                hash = nextHash(hash, data);
                data = alignedData >>> tailShift;
                position += ALIGNMENT_INT;
            }
            if (position == alignedEnd && tailLen > 0) {
                long alignedData = segment.tail();
                long shifted = alignedData << headShift;
                data |= shifted;
                hash = nextHash(hash, data);
                int headStart = ALIGNMENT_INT - headLen;
                int restTail = tailLen - headStart;
                if (restTail > 0) {
                    long remainingData = alignedData >> headStart * ALIGNMENT;
                    return nextHash(hash, remainingData);
                }
            } else if (headLen > 0) {
                return nextHash(hash, data);
            }
            return hash;
        }

        @Override
        public long getAsLong() {
            if (headLen >= length) {
                return Bits.truncate(data, length);
            }
            if (position < alignedEnd) {
                long alignedData = segment.longAt(position);
                long shifted = alignedData << headShift;
                data |= shifted;
                long value = data;
                data = alignedData >>> tailShift;
                position += ALIGNMENT;
                return value;
            }
            if (position == this.alignedEnd && tailLen > 0) {
                long alignedData = segment.tail();
                long shifted = alignedData << headShift;
                data |= shifted;
                position += ALIGNMENT_INT;
                return data;
            }
            if (position == endIndex) {
                return data;
            }
            if (tailLen > headStart) {
                return segment.tail() >> headStart * ALIGNMENT;
            }
            return 0x0L;
        }

        @Override
        protected ReusableBase initialize(
            LineSegment segment,
            int headLen,
            int headStart,
            long startIndex,
            long endIndex,
            int length
        ) {
            this.segment = segment;
            this.endIndex = endIndex;
            this.length = length;
            this.headLen = headLen;
            this.headStart = headStart;
            this.headShift = headLen * ALIGNMENT_INT;
            this.tailLen = (int) (this.endIndex % ALIGNMENT_INT);
            this.tailShift = (ALIGNMENT_INT - headLen) * ALIGNMENT_INT;

            this.alignedEnd = this.endIndex - this.endIndex % ALIGNMENT_INT;
            this.position = startIndex - this.headStart + (headLen > 0 ? ALIGNMENT : 0);

            this.data = this.segment.head();
            return this;
        }
    }

    private final class Aligned extends ReusableBase {

        private LineSegment segment;

        private long endIndex;

        private int headLen;

        private int tailLen;

        private long alignedStart;

        private long alignedEnd;

        private long position;

        @Override
        public long size() {
            return segment.alignedLongsCount();
        }

        @Override
        public void forEach(IndexedLongConsumer consumer) {
            int index = 0;
            for (long pos = alignedStart; pos < alignedEnd; pos += ALIGNMENT) {
                long data = segment.longAt(pos);
                consumer.accept(index++, data);
            }
            if (endIndex % ALIGNMENT > 0L) {
                long data = segment.tail();
                long truncated = Bits.truncate(data, tailLen);
                consumer.accept(index, truncated);
            }
        }

        @Override
        public int toHashCode() {
            int hash = 17;
            for (long pos = alignedStart; pos < alignedEnd; pos += ALIGNMENT) {
                long data = segment.longAt(pos);
                hash = nextHash(hash, data);
            }
            if (endIndex % ALIGNMENT > 0L) {
                long data = segment.tail();
                long truncated = Bits.truncate(data, tailLen);
                hash = nextHash(hash, truncated);
            }
            return hash;
        }

        @Override
        public long getAsLong() {
            if (position == alignedStart && headLen > 0) {
                long head = segment.head();
                position += ALIGNMENT;
                return head;
            }
            if (position < alignedEnd) {
                long data = segment.longAt(position);
                position += ALIGNMENT;
                return data;
            }
            if (position == alignedEnd && tailLen > 0) {
                return segment.tail();
            }
            return 0x0L;
        }

        @Override
        protected ReusableBase initialize(
            LineSegment segment,
            int headLen,
            int headStart,
            long startIndex,
            long endIndex,
            int length
        ) {
            this.segment = segment;
            this.endIndex = endIndex;
            this.headLen = headLen;
            this.alignedStart = startIndex - headStart;
            this.alignedEnd = this.endIndex - this.endIndex % ALIGNMENT_INT;
            this.tailLen = (int) (this.endIndex % ALIGNMENT_INT);
            this.position = this.alignedStart;
            return this;
        }
    }

    public sealed interface Reusable extends LongSupplier, Function<LineSegment, Reusable> {

        long size();

        void forEach(IndexedLongConsumer consumer);

        default int toHashCode(LineSegment lineSegment) {
            return apply(lineSegment).toHashCode();
        }

        default int toHashCode() {
            int hash = 17;
            long size = size();
            for (int i = 0; i < size; i++) {
                hash = nextHash(hash, getAsLong());
            }
            return hash;
        }
    }

    @FunctionalInterface
    public interface IndexedLongConsumer extends LongConsumer {

        default void accept(long value) {
            accept(-1, value);
        }

        void accept(int index, long value);
    }
}
