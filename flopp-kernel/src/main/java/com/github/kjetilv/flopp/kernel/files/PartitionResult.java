package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.partitions.Partition;

import java.util.function.Supplier;

record PartitionResult<T>(Partition partition, Supplier<T> completer, T result, boolean done)
    implements Comparable<PartitionResult<T>> {

    PartitionResult(Partition partition, T result) {
        this(partition, null, result, true);
    }

    PartitionResult(Partition partition, Supplier<T> completer) {
        this(partition, completer, null, false);
    }

    @SuppressWarnings("UnusedReturnValue")
    public PartitionResult<T> complete() {
        return done
            ? this
            : new PartitionResult<>(partition, null, completer.get(), true);
    }

    public PartitionResult<T> withAdjustedPartition(Partition adjusted) {
        return new PartitionResult<>(adjusted, completer, result, done);
    }

    @Override
    public T result() {
        return done ? result : completer().get();
    }

    @Override
    public int compareTo(PartitionResult o) {
        return partition.compareTo(o.partition());
    }
}
