package com.github.kjetilv.flopp.kernel.partitions;

import static com.github.kjetilv.flopp.kernel.MemorySegments.ALIGNMENT_INT;

public record Partitionings(int alignment) {

    public static Partitioning create() {
        return new Partitioning(Runtime.getRuntime().availableProcessors(), 0, null, ALIGNMENT_INT);
    }

    public static Partitioning create(int count) {
        return create(count, 0);
    }

    public static Partitioning create(int count, long tail) {
        return new Partitioning(count, tail, null, ALIGNMENT_INT);
    }

    public static Partitioning single() {
        return create(1);
    }
}
