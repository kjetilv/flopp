package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.bits.BitwisePartitionHandler.Action;
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

    private final MutableLine line = new MutableLine();

    private final MemorySegment segment;

    BitwisePartitionSpliterator(
        Partition partition,
        MemorySegment segment,
        Mediator mediator,
        BitwisePartitionSpliterator next
    ) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);
        this.partition = Objects.requireNonNull(partition, "partition");
        this.line.memorySegment = this.segment = segment;
        this.mediator = mediator;
        this.next = next;
    }

    @Override
    public boolean tryAdvance(Consumer<? super LineSegment> action) {
        try {
            Action forwarder = lineForwarder(action);
            Action mediated = mediated(forwarder);
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

    private Action mediated(Action forwarder) {
        return mediator == null ? forwarder : mediator.apply(forwarder);
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

    private Action lineForwarder(Consumer<? super LineSegment> action) {
        return (memorySegment, offset, length) -> {
            line.offset = offset;
            line.length = length;
            line.memorySegment = memorySegment;
            action.accept(line);
        };
    }

    private static final class MutableLine implements LineSegment {

        private MemorySegment memorySegment;

        private long offset;

        private long length;

        @Override
        public MemorySegment memorySegment() {
            return memorySegment;
        }

        @Override
        public long offset() {
            return offset;
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public String toString() {
            return STR."\{getClass().getSimpleName()}[\{offset()}+\{length()}]";
        }
    }
}
