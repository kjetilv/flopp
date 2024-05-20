package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.LineSegments;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.bits.BitwisePartitionHandler.MiddleMan;
import com.github.kjetilv.flopp.kernel.bits.BitwisePartitioned.Action;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.nio.charset.StandardCharsets.UTF_8;

final class BitwisePartitionSpliterator extends Spliterators.AbstractSpliterator<LineSegment> {

    private final Partition partition;

    private final MiddleMan<BitwisePartitioned.Action> middleMan;

    private final Supplier<BitwisePartitionSpliterator> next;

    private final boolean immutable;

    private MemorySegment segment;

    private final long logicalSize;

    private long underlyingSize;

    BitwisePartitionSpliterator(
        Partition partition,
        MemorySegment segment,
        long logicalSize,
        MiddleMan<BitwisePartitioned.Action> middleMan,
        Supplier<BitwisePartitionSpliterator> next,
        boolean immutable
    ) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);
        this.partition = Objects.requireNonNull(partition, "partition");
        this.logicalSize = logicalSize;
        this.middleMan = middleMan == null
            ? action -> action
            : middleMan;
        this.next = next;
        this.immutable = immutable;
        adopt(segment);
    }

    @Override
    public boolean tryAdvance(Consumer<? super LineSegment> action) {
        try {
            Action delegate = immutable
                ? new ImmutableForwarder(action)
                : new MutableForwarder(action);
            handler(middleMan.intercept(delegate)).run();
            return false;
        } catch (Exception e) {
            throw new IllegalStateException(this + " failed: " + action, e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[@" + partition + "]";
    }

    private BitwisePartitionHandler handler(Action action) {
        return new BitwisePartitionHandler(
            partition,
            segment,
            logicalSize,
            action,
            next == null
                ? null
                : () -> next.get().handler(action)
        );
    }

    private void adopt(MemorySegment segment) {
        this.segment = segment;
        this.underlyingSize = segment.byteSize();
    }

    private final class MutableForwarder implements Action, LineSegment {

        private final Consumer<? super LineSegment> action;

        private long startIndex;

        private long endIndex;

        MutableForwarder(Consumer<? super LineSegment> action) {
            this.action = action;
        }

        @Override
        public void line(MemorySegment memorySegment, long startIndex, long endIndex) {
            adopt(memorySegment);
            line(startIndex, endIndex);
        }

        @Override
        public void line(long startIndex, long endIndex) {
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            action.accept(this);
        }

        @Override
        public String asString() {
            return asString(null);
        }

        @Override
        public String asString(byte[] buffer) {
            return MemorySegments.fromLongsWithinBounds(segment, startIndex, endIndex, buffer, null);
        }

        @Override
        public long headStart() {
            return startIndex % ALIGNMENT;
        }

        @Override
        public boolean isAlignedAtStart() {
            return startIndex % ALIGNMENT == 0L;
        }

        @Override
        public boolean isAlignedAtEnd() {
            return endIndex % ALIGNMENT == 0;
        }

        @Override
        public MemorySegment memorySegment() {
            return segment;
        }

        @Override
        public long underlyingSize() {
            return underlyingSize;
        }

        @Override
        public long head(boolean truncate) {
            long head = startIndex % ALIGNMENT;
            long headLength = head == 0L ? 0L : ALIGNMENT - head;
            long nominalLength = endIndex - startIndex;
            long readLength = headLength == 0
                ? nominalLength
                : Math.min(headLength, nominalLength);
            if (underlyingSize - startIndex < ALIGNMENT) {
                return MemorySegments.readHead(segment, startIndex, readLength);
            }
            long value = segment.get(JAVA_LONG_UNALIGNED, startIndex);
            return truncate
                ? Bits.lowerBytes(value, Math.toIntExact(readLength))
                : value;
        }

        @Override
        public long tail(boolean truncate) {
            int tail = Math.toIntExact(endIndex % ALIGNMENT);
            if (underlyingSize - endIndex < ALIGNMENT) {
                return MemorySegments.readTail(segment, endIndex, tail);
            }
            long tailEnd = endIndex % ALIGNMENT;
            long value = segment.get(JAVA_LONG_UNALIGNED, endIndex - tailEnd);
            return truncate
                ? Bits.lowerBytes(value, tail)
                : value;
        }

        @Override
        public long head(long head) {
            long l = segment.get(JAVA_LONG, startIndex - startIndex % ALIGNMENT);
            return l >> head * ALIGNMENT;
        }

        @Override
        public long longNo(int longNo) {
            return segment.get(JAVA_LONG, startIndex - startIndex % ALIGNMENT + longNo * ALIGNMENT);
        }

        @Override
        public long startIndex() {
            return startIndex;
        }

        @Override
        public long endIndex() {
            return endIndex;
        }

        @Override
        public String toString() {
            return startIndex() + "+" + length() + ":" + LineSegments.asString(segment, startIndex, endIndex);
        }
    }

    private final class ImmutableForwarder implements Action {

        private final Consumer<? super LineSegment> action;

        ImmutableForwarder(Consumer<? super LineSegment> action) {
            this.action = action;
        }

        @Override
        public void line(long startIndex, long endIndex) {
            action.accept(LineSegments.of(segment, startIndex, endIndex));
        }

        @Override
        public void line(MemorySegment segment, long startIndex, long endIndex) {
            action.accept(LineSegments.of(segment, startIndex, endIndex));
        }
    }
}
