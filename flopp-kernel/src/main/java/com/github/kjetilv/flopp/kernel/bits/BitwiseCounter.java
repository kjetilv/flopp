package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;

import java.lang.foreign.MemorySegment;
import java.util.Objects;

@SuppressWarnings("StringTemplateMigration")
final class BitwiseCounter {

    private final Partition partition;

    private final MemorySegmentSource memorySegmentSource;

    private final BitwiseCounter next;

    BitwiseCounter(
        Partition partition,
        MemorySegmentSource memorySegmentSource,
        BitwiseCounter next
    ) {
        this.partition = Objects.requireNonNull(partition, "partition");
        this.memorySegmentSource = Objects.requireNonNull(memorySegmentSource, "memorySegmentSource");
        this.next = next;
    }

    public long count() {
        try {
            Counter action = new Counter();
            handler(action).run();
            return action.lc;
        } catch (Exception e) {
            throw new IllegalStateException(this + " failed", e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[@" + partition + "]";
    }

    private BitwisePartitionHandler handler(BitwisePartitioned.Action action) {
        return new BitwisePartitionHandler(
            partition,
            memorySegmentSource.get(partition),
            action,
            next == null ? null : () -> next.handler(action)
        );
    }

    private static final class Counter implements BitwisePartitioned.Action {

        private long lc;

        @Override
        public void line(MemorySegment memorySegment, long startIndex, long endIndex) {
            lc++;
        }
    }
}
