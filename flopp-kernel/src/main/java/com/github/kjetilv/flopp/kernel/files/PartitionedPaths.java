package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Partitioned;
import com.github.kjetilv.flopp.kernel.Shape;
import com.github.kjetilv.flopp.kernel.partitions.Partitioning;

import java.nio.file.Path;

public final class PartitionedPaths {

    public static Partitioned<Path> partitioned(Path path) {
        return partitioned(path, (Partitioning) null);
    }

    public static Partitioned<Path> partitioned(Path path, Partitioning partitioning) {
        return partitioned(path, partitioning, null);
    }

    public static Partitioned<Path> partitioned(Path path, Shape shape) {
        return partitioned(path, null, shape);
    }

    public static Partitioned<Path> partitioned(Path path, Partitioning partitioning, Shape shape) {
        return new PartitionedPath(path, partitioning, shape);
    }

    private PartitionedPaths() {
    }
}
