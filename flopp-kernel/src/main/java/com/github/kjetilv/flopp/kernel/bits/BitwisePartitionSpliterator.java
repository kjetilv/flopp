package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.bits.BitwisePartitionHandler.Mediator;
import com.github.kjetilv.flopp.kernel.bits.BitwisePartitioned.Action;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;

@SuppressWarnings("StringTemplateMigration")
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
}
