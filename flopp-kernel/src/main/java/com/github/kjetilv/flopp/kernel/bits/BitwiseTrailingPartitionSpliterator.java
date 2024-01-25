package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;

import java.lang.foreign.MemorySegment;
import java.util.function.Consumer;

public final class BitwiseTrailingPartitionSpliterator extends AbstractBitwisePartitionSpliterator {

    private final int tail;

    @Override
    protected String toStringAddendum() {
        return STR."+\{tail}";
    }

    public BitwiseTrailingPartitionSpliterator(Partition partition, MemorySegment ms) {
        super(partition, ms);
        this.tail = Math.toIntExact(partition.count() % partition.alignment());
    }

    @Override
    public boolean tryAdvance(Consumer<? super LineSegment> action) {
        skipToStart();
        processTail(action, tail);
        return false;
    }
}
