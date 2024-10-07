package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.bits.BitwisePartitioned.Action;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

final class BitwisePartitionLineSpliterator extends Spliterators.AbstractSpliterator<LineSegment> {

    private final Partition partition;

    private final Function<Consumer<LineSegment>, Action> headersAndFooters;

    private final Supplier<BitwisePartitionLineSpliterator> next;

    private final MemorySegment segment;

    private final long offset;

    private final long logicalSize;

    BitwisePartitionLineSpliterator(
        Partition partition,
        MemorySegment segment,
        long offset,
        long logicalSize,
        Function<Consumer<LineSegment>, Action> headersAndFooters,
        Supplier<BitwisePartitionLineSpliterator> next
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
        try {
            if (headersAndFooters == null) {
                feeder((Consumer<LineSegment>) action).run();
            } else {
                try (Action wrapped = headersAndFooters.apply((Consumer<LineSegment>) action)) {
                    feeder(wrapped).run();
                }
            }
            return false;
        } catch (Exception e) {
            throw new IllegalStateException(this + " failed: " + action, e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[@" + partition + "]";
    }

    private BitwisePartitionLineFeeder feeder(Consumer<LineSegment> action) {
        Supplier<BitwisePartitionLineFeeder> supplier = next == null
            ? null
            : () -> next.get().feeder(action);
        return new BitwisePartitionLineFeeder(partition, segment, offset, logicalSize, action, supplier);
    }
}
