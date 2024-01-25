package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;

import java.lang.foreign.MemorySegment;
import java.util.function.Consumer;

final class BitwiseInitialPartitionSpliterator
    extends AbstractBitwisePartitionSpliterator {

    BitwiseInitialPartitionSpliterator(Partition partition, MemorySegment ms) {
        super(partition, ms);
    }

    @Override
    public boolean tryAdvance(Consumer<? super LineSegment> action) {
        processAligned(action);
        return false;
    }
}
