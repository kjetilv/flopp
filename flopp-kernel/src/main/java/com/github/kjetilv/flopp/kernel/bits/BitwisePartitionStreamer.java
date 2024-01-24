package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class BitwisePartitionStreamer {

    private final MemorySegment memorySegment;

    private final Partition partition;

    public BitwisePartitionStreamer(Partition partition, MemorySegment memorySegment) {
        this.partition = Objects.requireNonNull(partition, "partition");
        this.memorySegment = memorySegment;
    }

    public Stream<LineSegment> lines() {
        return StreamSupport.stream(
            partition.first() ? new BitwiseInitialPartitionSpliterator(partition, memorySegment)
                : !partition.last() ? new BitwiseLineSeekPartitionSpliterator(partition, memorySegment)
                    : new BitwiseTrailingPartitionSpliterator(partition, memorySegment),
            false
        );
    }

    public Partition partition() {
        return partition;
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{partition}]";
    }
}
