package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.LineSegments;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.bits.BitwisePartitionHandler.Mediator;
import com.github.kjetilv.flopp.kernel.bits.BitwisePartitioned.Action;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;

final class BitwisePartitionSpliterator extends Spliterators.AbstractSpliterator<LineSegment> {

    private final Partition partition;

    private final Mediator mediator;

    private final BitwisePartitionSpliterator next;

    private final boolean immutable;

    private final MemorySegment segment;

    BitwisePartitionSpliterator(
        Partition partition,
        MemorySegment segment,
        Mediator mediator,
        BitwisePartitionSpliterator next,
        boolean immutable
    ) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);
        this.partition = Objects.requireNonNull(partition, "partition");
        this.segment = segment;
        this.mediator = mediator == null
            ? action -> action
            : mediator;
        this.next = next;
        this.immutable = immutable;
    }

    @Override
    public boolean tryAdvance(Consumer<? super LineSegment> action) {
        try {
            Action mediated = mediator.mediate(forwarder(action));
            BitwisePartitionHandler handler = handler(mediated);
            handler.run();
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

    private Action forwarder(Consumer<? super LineSegment> action) {
        return immutable
            ? new ImmutableForwarder(action)
            : new MutableForwarder(action);
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
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.memorySegment = memorySegment;
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
        public long startIndex() {
            return startIndex;
        }

        @Override
        public long endIndex() {
            return endIndex;
        }

        @Override
        public int hashCode() {
            return Objects.hash(memorySegment, startIndex, endIndex);
        }

        @Override
        public boolean equals(Object obj) {
            return obj == this ||
                   obj instanceof LineSegment ls && memorySegment().equals(ls.memorySegment()) &&
                   startIndex() == ls.startIndex() &&
                   endIndex() == ls.endIndex();
        }

        @Override
        public String toString() {
            return LineSegments.asString(memorySegment, startIndex, endIndex);
        }
    }
}
