package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.MemorySegmentSource;
import com.github.kjetilv.flopp.kernel.Partitioned;
import com.github.kjetilv.flopp.kernel.Shape;
import com.github.kjetilv.flopp.kernel.partitions.Partitioning;

public final class Partitioneds {

    public static Partitioned create(
        Partitioning partitioning,
        Shape resolved,
        MemorySegmentSource segmentSource
    ) {
        return new BitwisePartitioned(partitioning, resolved, segmentSource);
    }

    public static Partitioned createVectorized(
        Partitioning partitioning,
        Shape resolved,
        MemorySegmentSource segmentSource
    ) {
        return new VectorPartitioned(partitioning, resolved, segmentSource);
    }

    private Partitioneds() {
    }
}
