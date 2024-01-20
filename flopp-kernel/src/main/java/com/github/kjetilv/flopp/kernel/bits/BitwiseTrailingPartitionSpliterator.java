package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;

import java.lang.foreign.MemorySegment;
import java.util.function.Consumer;

public final class BitwiseTrailingPartitionSpliterator extends AbstractBitwisePartitionSpliterator {

    private final int trail;

    public BitwiseTrailingPartitionSpliterator(Partition partition, MemorySegment ms) {
        super(partition, ms);
        this.trail = Math.toIntExact(partition.count() % partition.alignment());
        this.partitionLimit = partition.count() - this.trail;
    }

    @Override
    boolean advance(Consumer<? super MemorySegments.LineSegment> action) {
        current = next();
        jumpToLine();
        processToTail(action, trail);
        while (true) {
            if (cycleDone(action)) {
                return false;
            }
        }
    }
}
