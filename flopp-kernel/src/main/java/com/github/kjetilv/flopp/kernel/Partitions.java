package com.github.kjetilv.flopp.kernel;

import java.util.List;
import java.util.Objects;

public record Partitions(Partitioning partitioning, long total, List<Partition> partitions) {

    public Partitions(Partitioning partitioning, long total, List<Partition> partitions) {
        this.partitioning = Objects.requireNonNull(partitioning, "partitioning");
        this.total = Non.negative(total, "total");
        this.partitions = Non.empty(partitions, "partitions").stream().sorted().toList();
    }

    public int size() {
        return partitions.size();
    }

    public Partition get(int index) {
        return partitions.get(index);
    }

    public Partition getFirst() {
        return partitions.getFirst();
    }

    public Partition getLast() {
        return partitions.getLast();
    }
}
