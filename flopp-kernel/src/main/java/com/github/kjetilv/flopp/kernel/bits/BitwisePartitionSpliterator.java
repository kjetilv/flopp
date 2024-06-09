package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.bits.BitwisePartitioned.Action;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

final class BitwisePartitionSpliterator extends Spliterators.AbstractSpliterator<LineSegment> {

    private final Partition partition;

    private final Function<Consumer<LineSegment>, Action> headersAndFooters;

    private final Supplier<BitwisePartitionSpliterator> next;

    private final MemorySegment segment;

    private final long offset;

    private final long logicalSize;

    BitwisePartitionSpliterator(
        Partition partition,
        MemorySegment segment,
        long offset,
        long logicalSize,
        Function<Consumer<LineSegment>, Action> headersAndFooters,
        Supplier<BitwisePartitionSpliterator> next
    ) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);
        this.partition = Objects.requireNonNull(partition, "partition");
        this.offset = offset;
        this.logicalSize = logicalSize;
        this.headersAndFooters = headersAndFooters;
        this.next = next;
        this.segment = segment;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean tryAdvance(Consumer<? super LineSegment> action) {
        if (headersAndFooters == null) {
            handler(action::accept).run();
        } else {
            try (Action wrapped = headersAndFooters.apply((Consumer<LineSegment>) action)) {
                handler(wrapped).run();
            } catch (Exception e) {
                throw new IllegalStateException(this + " failed: " + action, e);
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[@" + partition + "]";
    }

    private BitwisePartitionHandler handler(Consumer<LineSegment> action) {
        return new BitwisePartitionHandler(
            partition,
            segment,
            offset,
            logicalSize,
            action,
            next == null ? null : () -> next.get().handler(action)
        );
    }
}
