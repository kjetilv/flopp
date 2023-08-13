package com.github.kjetilv.lopp;

import com.github.kjetilv.lopp.utils.Non;

import java.util.Objects;

public record PartitionBytes(long offset, long count, Partition partition) implements Comparable<PartitionBytes> {

    static PartitionBytes create(long startOffset, long endOffset, Partition partition) {
        return new PartitionBytes(
            Non.negative(startOffset, "byteOffset"),
            Non.negative(endOffset, "endOffset") - startOffset,
            Objects.requireNonNull(partition, "partition")
        );
    }

    public PartitionBytes(long offset, long count, Partition partition) {
        this.partition = Objects.requireNonNull(partition, "partition");
        this.offset = Non.negative(offset, "offset");
        this.count = validLength(Non.negative(count, "count"));
    }

    @Override
    public int compareTo(PartitionBytes partitionBytes) {
        return partition.compareTo(partitionBytes.partition());
    }

    public boolean empty() {
        return partition().empty();
    }

    public Partition toPartition() {
        return new Partition(partition.partitionNo(), partition.partitionCount(), offset, count);
    }

    public PartitionBytes at(long offset, long count) {
        return new PartitionBytes(offset, count, partition);
    }

    private static long validLength(long byteLength) {
        if (byteLength > Integer.MAX_VALUE) {
            int excess = 100 - Double.valueOf(100.0 * byteLength / Integer.MAX_VALUE).intValue();
            throw new IllegalStateException(
                "Invalid partition size " + byteLength + ", exceeds Integer.MAX_VALUE by " + excess + "%");
        }
        return byteLength;
    }

    @Override public String toString() {
        return getClass().getSimpleName() + "[" + offset + "+" + count + ": " + partition + "]";
    }
}
