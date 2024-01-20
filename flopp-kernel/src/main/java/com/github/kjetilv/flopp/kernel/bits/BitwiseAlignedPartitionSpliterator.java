package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;

import java.lang.foreign.MemorySegment;
import java.util.function.Consumer;

final class BitwiseAlignedPartitionSpliterator
    extends AbstractBitwisePartitionSpliterator {

    private final boolean shifted;

    BitwiseAlignedPartitionSpliterator(Partition partition, MemorySegment ms) {
        super(partition, ms);
        this.partitionLimit = partition.count();
        shifted = !partition.first();
    }

    @Override
    boolean advance(Consumer<? super MemorySegments.LineSegment> action) {
        current = next();
        if (shifted) {
            jumpToLine();
        }
        while (true) {
            if (cycleDone(action)) {
                return false;
            }
        }
    }
}
