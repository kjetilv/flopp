package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
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
        long result;
        try {
            result = ms.get(ValueLayout.JAVA_LONG, byteOffset);
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed to advance from \{byteOffset} in \{ms}", e);
        }
        current = result;
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
