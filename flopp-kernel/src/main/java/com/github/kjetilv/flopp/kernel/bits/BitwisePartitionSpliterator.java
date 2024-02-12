package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.bits.BitwisePartitioned.Action;
import com.github.kjetilv.flopp.kernel.bits.BitwisePartitionHandler.Mediator;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;

final class BitwisePartitionSpliterator
    extends Spliterators.AbstractSpliterator<LineSegment> {

    private final Partition partition;

    private final Mediator mediator;

    private final BitwisePartitionSpliterator next;

    private final boolean copying;

    private final MemorySegment segment;

    BitwisePartitionSpliterator(
        Partition partition,
        MemorySegment segment,
        Mediator mediator,
        BitwisePartitionSpliterator next,
        boolean copying
    ) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);
        this.partition = Objects.requireNonNull(partition, "partition");
        this.segment = segment;
        this.mediator = mediator;
        this.next = next;
        this.copying = copying;
    }

    @Override
    public boolean tryAdvance(Consumer<? super LineSegment> action) {
        try {
            Action forwarder = copying ? new CopyingForwarder(action) : new MutationForwarder(action);
            Action mediated = mediator == null ? forwarder : mediator.apply(forwarder);
            handler(mediated).run();
            return false;
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed: \{action}", e);
        }
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[@\{partition}]";
    }

    private BitwisePartitionHandler handler(Action action) {
        return new BitwisePartitionHandler(
            partition,
            segment,
            action,
            next == null
                ? null
                : () -> next.handler(action)
        );
    }

    private static final class CopyingForwarder implements Action {

        private final Consumer<? super LineSegment> action;

        private CopyingForwarder(Consumer<? super LineSegment> action) {
            this.action = action;
        }

        @Override
        public void line(MemorySegment segment, long startIndex, long endIndex) {
            action.accept(LineSegment.ofRange(segment, startIndex, endIndex));
        }
    }

    private static final class MutationForwarder implements Action, LineSegment {

        private final Consumer<? super LineSegment> action;

        private MemorySegment memorySegment;

        private long lineNo;

        private long startIndex;

        private long endIndex;

        private MutationForwarder(Consumer<? super LineSegment> action) {
            this.action = Objects.requireNonNull(action, "action");
        }

        @Override
        public void line(MemorySegment memorySegment, long startIndex, long endIndex) {
            this.lineNo++;
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.memorySegment = memorySegment;
            action.accept(this);
        }

        @Override
        public long lineNo() {
            return lineNo;
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
        public boolean equals(Object obj) {
            return obj == this || obj instanceof LineSegment lineSegment && lineSegment.asString().equals(asString());
        }

        @Override
        public int hashCode() {
            return asString().hashCode();
        }

        @Override
        public String toString() {
            return LineSegment.toString(memorySegment, startIndex, endIndex);
        }
    }
}
