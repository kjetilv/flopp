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

final class BitwisePartitionSpliterator extends Spliterators.AbstractSpliterator<LineSegment> {

    private final Partition partition;

    private final MiddleMan<BitwisePartitioned.Action> middleMan;

    private final BitwisePartitionSpliterator next;

    private final boolean immutable;

    private final MemorySegment segment;

    BitwisePartitionSpliterator(
        Partition partition,
        MemorySegment segment,
        MiddleMan<BitwisePartitioned.Action> middleMan,
        BitwisePartitionSpliterator next,
        boolean immutable
    ) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);
        this.partition = Objects.requireNonNull(partition, "partition");
        this.segment = segment;
        this.middleMan = middleMan == null
            ? action -> action
            : middleMan;
        this.next = next;
        this.immutable = immutable;
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

    BitwisePartitionHandler handler(Action action) {
        return new BitwisePartitionHandler(
            partition,
            segment,
            action,
            next == null ? null : () -> next.handler(action)
        );
    }

    private static final class ImmutableForwarder implements Action {

        private final Consumer<? super LineSegment> action;

        ImmutableForwarder(Consumer<? super LineSegment> action) {
            this.action = action;
        }

        @Override
        public void line(MemorySegment segment, long startIndex, long endIndex) {
            action.accept(LineSegments.of(segment, startIndex, endIndex));
        }
    }

    private static final class MutableForwarder implements Action, LineSegment {

        private final Consumer<? super LineSegment> action;

        private MemorySegment memorySegment;

        private long startIndex;

        private long endIndex;

        MutableForwarder(Consumer<? super LineSegment> action) {
            this.action = Objects.requireNonNull(action, "action");
        }

        @Override
        public void line(MemorySegment memorySegment, long startIndex, long endIndex) {
            if (endIndex < startIndex) {
                throw new IllegalArgumentException(endIndex + " < " + startIndex);
            }
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.memorySegment = Objects.requireNonNull(memorySegment, "memorySegment");
            action.accept(this);
        }

        @Override
        public void close() {
            this.startIndex = 0;
            this.endIndex = 0;
            this.memorySegment = null;
        }

        @Override
        public MemorySegment memorySegment() {
            return memorySegment;
        }

        @Override
        public long underlyingSize() {
            return memorySegment.byteSize();
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
            return memorySegment == null
                ? "CLOSED"
                : LineSegments.asString(memorySegment, startIndex, endIndex);
        }
    }
}
