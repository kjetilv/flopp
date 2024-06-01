package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.bits.BitwisePartitionHandler.MiddleMan;
import com.github.kjetilv.flopp.kernel.bits.BitwisePartitioned.Action;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Supplier;

final class BitwisePartitionSpliterator extends Spliterators.AbstractSpliterator<LineSegment> {

    private final Partition partition;

    private final MiddleMan<BitwisePartitioned.Action> middleMan;

    private final Supplier<BitwisePartitionSpliterator> next;

    private final MemorySegment segment;

    private final long logicalSize;

    BitwisePartitionSpliterator(
        Partition partition,
        MemorySegment segment,
        long logicalSize,
        MiddleMan<BitwisePartitioned.Action> middleMan,
        Supplier<BitwisePartitionSpliterator> next
    ) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);
        this.partition = Objects.requireNonNull(partition, "partition");
        this.logicalSize = logicalSize;
        this.middleMan = middleMan;
        this.next = next;
        this.segment = segment;
    }

    @Override
    public boolean tryAdvance(Consumer<? super LineSegment> action) {
        try {
            Action a = middleMan == null
                ? action::accept
                : middleMan.intercept(action::accept);
            BitwisePartitionHandler handler = handler(a);
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

}
