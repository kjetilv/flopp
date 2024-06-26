package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;

import java.lang.foreign.MemorySegment;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import static com.github.kjetilv.flopp.kernel.LineSegments.nextHash;
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
        long head = segment.startIndex() % ALIGNMENT_INT;
        int length = (int) (endIndex - startIndex);
        if (length == 0) {
            return empty;
        }
        int headLen = head == 0L
            ? 0
            : ALIGNMENT_INT - (int) head;
        return baseFor(headLen).initialize(segment, headLen, startIndex, endIndex, length);
    }

    abstract ReusableBase baseFor(int headLen);

    private static final class AlignedTraverser extends BitwiseTraverser {

        @Override
        ReusableBase baseFor(int headLen) {
            return aligned;
        }
    }

    private static final class MultiModeSuppler extends BitwiseTraverser {

        @Override
        ReusableBase baseFor(int headLen) {
            return headLen == 0 ? aligned : shifted;
        }
    }

    private abstract sealed class ReusableBase implements Reusable {

        @Override
        public final Reusable apply(LineSegment lineSegment) {
            return BitwiseTraverser.this.apply(lineSegment);
        }

        Reusable initialize(LineSegment segment, int headLen, long startIndex, long endIndex, int length) {
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
        public long toHashCode() {
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

        private MemorySegment memorySegment;

        private int length;

        private int headLen;

        private int tailLen;

        private int headStart;

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
                long alignedData = memorySegment.get(JAVA_LONG, position);
                long shifted = alignedData << headShift;
                data |= shifted;
                consumer.accept(index++, data);
                data = alignedData >>> tailShift;
                position += ALIGNMENT_INT;
            }
            if (position == alignedEnd && tailLen > 0) {
                long alignedData = MemorySegments.tail(memorySegment, endIndex);
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
        public long toHashCode() {
            long hash = 0;
            long data = segment.head();
            if (headLen >= length) {
                hash = nextHash(hash, Bits.truncate(data, length));
                return hash;
            }
            while (position < alignedEnd) {
                long alignedData = memorySegment.get(JAVA_LONG, position);
                long shifted = alignedData << headShift;
                data |= shifted;
                hash = nextHash(hash, data);
                data = alignedData >>> tailShift;
                position += ALIGNMENT_INT;
            }
            if (position == alignedEnd && tailLen > 0) {
                long alignedData = MemorySegments.tail(memorySegment, endIndex);
                long shifted = alignedData << headShift;
                data |= shifted;
                hash = nextHash(hash, data);
                int headStart = ALIGNMENT_INT - headLen;
                int restTail = tailLen - headStart;
                if (restTail > 0) {
                    long remainingData = alignedData >> headStart * ALIGNMENT;
                    hash = nextHash(hash, remainingData);
                }
            } else if (headLen > 0) {
                hash = nextHash(hash, data);
            }
            return hash;
        }

        @Override
        public long getAsLong() {
            if (headLen >= length) {
                return Bits.truncate(data, length);
            }
            if (position < alignedEnd) {
                long alignedData = memorySegment.get(JAVA_LONG, position);
                long shifted = alignedData << headShift;
                data |= shifted;
                long value = data;
                data = alignedData >>> tailShift;
                position += ALIGNMENT;
                return value;
            }
            if (position == this.alignedEnd && tailLen > 0) {
                long alignedData = MemorySegments.tail(memorySegment, endIndex);
                long shifted = alignedData << headShift;
                data |= shifted;
                position += ALIGNMENT_INT;
                return data;
            }
            if (position == endIndex) {
                return data;
            }
            if (tailLen > headStart) {
                int restTail = tailLen - headStart;
                if (restTail > 0) {
                    long alignedData = MemorySegments.tail(memorySegment, endIndex);
                    long remainingData = alignedData >> headStart * ALIGNMENT;
                    return remainingData;
                }
            }
            return 0x0L;
        }

        @Override
        ReusableBase initialize(LineSegment segment, int headLen, long startIndex, long endIndex, int length) {
            this.segment = segment;
            this.endIndex = endIndex;
            this.length = length;
            this.memorySegment = segment.memorySegment();
            this.headLen = headLen;
            this.headStart = (int) (startIndex % ALIGNMENT_INT);
            this.headShift = headLen * ALIGNMENT_INT;
            this.tailLen = (int) (this.endIndex % ALIGNMENT_INT);
            this.tailShift = (ALIGNMENT_INT - headLen) * ALIGNMENT_INT;

            this.alignedEnd = this.endIndex - this.endIndex % ALIGNMENT_INT;
            this.position = startIndex - headStart + (headLen > 0 ? ALIGNMENT : 0);

            this.data = this.segment.head();
            return this;
        }
    }

    private final class Aligned extends ReusableBase {

        private LineSegment segment;

        private MemorySegment memorySegment;

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
                long data = memorySegment.get(JAVA_LONG, pos);
                consumer.accept(index++, data);
            }
            if (endIndex % ALIGNMENT > 0L) {
                long data = MemorySegments.tail(memorySegment, endIndex);
                long truncated = Bits.truncate(data, tailLen);
                consumer.accept(index, truncated);
            }
        }

        @Override
        public long toHashCode() {
            long hash = 0;
            for (long pos = alignedStart; pos < alignedEnd; pos += ALIGNMENT) {
                long data = memorySegment.get(JAVA_LONG, pos);
                hash = nextHash(hash, data);
            }
            if (endIndex % ALIGNMENT > 0L) {
                long data = MemorySegments.tail(memorySegment, endIndex);
                long truncated = Bits.truncate(data, tailLen);
                hash = nextHash(hash, truncated);
            }
            return hash;
        }

        @Override
        public long getAsLong() {
            long next = position == alignedStart && headLen > 0 ? segment.head() << ALIGNMENT * (ALIGNMENT - headLen)
                : position < alignedEnd ? memorySegment.get(JAVA_LONG, position)
                    : position == alignedEnd && tailLen > 0 ? MemorySegments.tail(memorySegment, endIndex)
                        : 0x0L;
            position += ALIGNMENT;
            return next;
        }

        @Override
        ReusableBase initialize(LineSegment segment, int headLen, long startIndex, long endIndex, int length) {
            this.segment = segment;
            this.endIndex = endIndex;
            this.memorySegment = this.segment.memorySegment();
            this.headLen = headLen;
            int headStart = (int) (startIndex % ALIGNMENT_INT);
            this.alignedStart = startIndex - headStart;
            this.alignedEnd = this.endIndex - this.endIndex % ALIGNMENT_INT;
            this.tailLen = (int) (this.endIndex % ALIGNMENT_INT);
            this.position = this.alignedStart;
            return this;
        }
    }

    public interface Reusable extends LongSupplier, Function<LineSegment, Reusable> {

        default Reusable reset(LineSegment segment) {
            return apply(segment);
        }

        long size();

        void forEach(IndexedLongConsumer consumer);

        default long[] fill(long[] buffer) {
            forEach((index, value) -> buffer[index] = value);
            return buffer;
        }

        default long toHashCode() {
            long hash = 0;
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
