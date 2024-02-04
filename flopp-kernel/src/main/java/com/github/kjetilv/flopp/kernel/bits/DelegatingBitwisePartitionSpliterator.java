package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.Partition;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;

public final class DelegatingBitwisePartitionSpliterator extends Spliterators.AbstractSpliterator<LineSegment> {

    private final Partition partition;

    private final ActionMediator actionMediator;

    private final DelegatingBitwisePartitionSpliterator next;

    private final MutableLine line = new MutableLine();

    private final MemorySegment segment;

    public DelegatingBitwisePartitionSpliterator(
        Partition partition,
        MemorySegment segment,
        ActionMediator actionMediator,
        DelegatingBitwisePartitionSpliterator next
    ) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);
        this.partition = Objects.requireNonNull(partition, "partition");
        this.line.memorySegment = this.segment = segment;
        this.actionMediator = actionMediator;
        this.next = next;
    }

    @Override
    public boolean tryAdvance(Consumer<? super LineSegment> action) {
        try {
            handler(mediated(lineForwarder(action))).run();
            return false;
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed: \{action}", e);
        }
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[@\{partition}]";
    }

    private BitwisePartitionHandler.Action mediated(BitwisePartitionHandler.Action forwarder) {
        return actionMediator == null ? forwarder : actionMediator.apply(forwarder);
    }

    private BitwisePartitionHandler handler(BitwisePartitionHandler.Action action) {
        return new BitwisePartitionHandler(partition, segment, action, next == null ? null : next.handler(action));
    }

    private BitwisePartitionHandler.Action lineForwarder(Consumer<? super LineSegment> action) {
        return (memorySegment, offset, length) -> {
            line.offset = offset;
            line.length = length;
            line.memorySegment = memorySegment;
            action.accept(line);
        };
    }
}
