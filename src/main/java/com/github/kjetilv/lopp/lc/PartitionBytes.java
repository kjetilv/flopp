package com.github.kjetilv.lopp.lc;

import com.github.kjetilv.lopp.Non;
import com.github.kjetilv.lopp.Partition;

import java.util.Objects;

public record PartitionBytes(long offset, long count, Partition partition) implements Comparable<PartitionBytes> {

    public PartitionBytes(long offset, long count, Partition partition) {
        this.partition = Objects.requireNonNull(partition, "partition");
        this.offset = Non.negative(offset, "offset");
        this.count = validLength(Non.negative(count, "count"));
    }

    @Override
    public int compareTo(PartitionBytes partitionBytes) {
        return partition.compareTo(partitionBytes.partition());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + offset + "+" + count + ": " + partition + "]";
    }

    private static long validLength(long byteLength) {
        if (byteLength > Integer.MAX_VALUE) {
            int excess = 100 - Double.valueOf(100.0 * byteLength / Integer.MAX_VALUE).intValue();
            throw new IllegalStateException(
                "Invalid partition size " + byteLength + ", exceeds Integer.MAX_VALUE by " + excess + "%");
        }
        return byteLength;
    }
}
