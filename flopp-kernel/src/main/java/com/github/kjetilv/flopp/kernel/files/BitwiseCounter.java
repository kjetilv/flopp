package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.CloseableConsumer;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

final class BitwiseCounter {

    private final Partition partition;

    private final MemorySegmentSource memorySegmentSource;

    private final Supplier<BitwiseCounter> next;

    BitwiseCounter(Partition partition, MemorySegmentSource memorySegmentSource, Supplier<BitwiseCounter> next) {
        this.partition = Objects.requireNonNull(partition, "partition");
        this.memorySegmentSource = Objects.requireNonNull(memorySegmentSource, "memorySegmentSource");
        this.next = next;
    }

    public long count() {
        try {
            Counter<LineSegment> action = new Counter<>();
            feeder(action).run();
            return action.lc;
        } catch (Exception e) {
            throw new IllegalStateException(this + " failed", e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[@" + partition + "]";
    }

    private BitwisePartitionLineFeeder feeder(Consumer<LineSegment> action) {
        LineSegment sourced = memorySegmentSource.get(partition);
        MemorySegment memorySegment = sourced.memorySegment();
        return new BitwisePartitionLineFeeder(
            partition,
            memorySegment,
            sourced.startIndex(),
            memorySegment.byteSize(),
            action,
            next == null
                ? null
                : () -> next.get().feeder(action)
        );
    }

    private static final class Counter<T> implements CloseableConsumer<T> {

        private long lc;

        @Override
        public void accept(T lineSegment) {
            lc++;
        }
    }
}
