package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.partitions.Partition;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.util.CloseableConsumer;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

final class BitwisePartitionLineSpliterator extends Spliterators.AbstractSpliterator<LineSegment> {

    private final Partition partition;

    private final Function<Consumer<LineSegment>, CloseableConsumer<LineSegment>> headersAndFooters;

    private final Supplier<BitwisePartitionLineSpliterator> next;

    private final MemorySegment segment;

    private final long offset;

    private final long logicalSize;

    BitwisePartitionLineSpliterator(
        Partition partition,
        MemorySegment segment,
        long offset,
        long logicalSize,
        Function<Consumer<LineSegment>, CloseableConsumer<LineSegment>> headersAndFooters,
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
                try (
                    CloseableConsumer<LineSegment> wrapped =
                        headersAndFooters.apply((Consumer<LineSegment>) action)
                ) {
                    feeder(wrapped).run();
                }
            }
            return false;
        } catch (Exception e) {
            throw new IllegalStateException(this + " failed: " + action, e);
        }
    }

    private BitwisePartitionLineFeeder feeder(Consumer<LineSegment> action) {
        Supplier<BitwisePartitionLineFeeder> supplier = next == null
            ? null
            : () -> next.get().feeder(action);
        return new BitwisePartitionLineFeeder(partition, segment, offset, logicalSize, action, supplier);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[@" + partition + "]";
    }
}
