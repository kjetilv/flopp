package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.MemorySegmentSource;
import com.github.kjetilv.flopp.kernel.Partitioned;
import com.github.kjetilv.flopp.kernel.PartitionedProcessors;
import com.github.kjetilv.flopp.kernel.Shape;
import com.github.kjetilv.flopp.kernel.partitions.Partitioning;

import java.nio.file.Path;

@SuppressWarnings("unused")
public final class PartitionedPaths {

    public static Partitioned partitioned(Path path) {
        return partitioned(path, (Partitioning) null);
    }

    public static Partitioned partitioned(Path path, Partitioning partitioning) {
        return partitioned(path, partitioning, null);
    }

    public static Partitioned partitioned(Path path, Shape shape) {
        return partitioned(path, null, shape);
    }

    public static Partitioned partitioned(Path path, Partitioning partitioning, Shape shape) {
        Shape resolved = shape == null ? Shape.of(path) : shape;
        return Partitioneds.create(
            partitioning,
            resolved,
            partialMemorySegmentSource(path, resolved)
        );
    }

    public static Partitioned vectorPartitioned(Path path, Partitioning partitioning, Shape shape) {
        Shape resolved = shape == null ? Shape.of(path) : shape;
        return Partitioneds.create(
            partitioning,
            resolved,
            fullMemorySegmentSource(path, resolved)
        );
    }

    public static PartitionedProcessors<Path> partitionedProcessors(
        Path path,
        Partitioning partitioning,
        Shape shape
    ) {
        return new PartitionedPathProcessors(path, partitioning, shape);
    }

    public static MemorySegmentSource partialMemorySegmentSource(Path path, Shape shape) {
        return new PartialMemorySegmentSource(path, shape);
    }

    public static MemorySegmentSource fullMemorySegmentSource(Path path, Shape shape) {
        return new FullMemorySegmentSource(path, shape);
    }

    private PartitionedPaths() {
    }
}
