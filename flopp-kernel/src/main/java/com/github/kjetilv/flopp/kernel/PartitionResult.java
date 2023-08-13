package com.github.kjetilv.flopp.kernel;

import java.util.Objects;

public record PartitionResult<T>(Partition partition, T result) implements Comparable<PartitionResult<T>> {

    public PartitionResult(Partition partition, T result) {
        this.partition = Objects.requireNonNull(partition, "partition");
        this.result = result;
    }

    @Override
    public int compareTo(PartitionResult o) {
        return partition.compareTo(o.partition());
    }

    public PartitionResult<T> withAdjustedPartition(Partition adjusted) {
        return new PartitionResult<>(adjusted, result);
    }
}
