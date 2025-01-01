package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.partitions.Partitioning;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.stream.Stream;

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
        MemorySegmentSource memorySegmentSource = new PartialMemorySegmentSource(path, resolved);
        return new PartitionedImpl(partitioning, resolved, memorySegmentSource);
    }

    public static PartitionedProcessors<Path> partitionedProcessors(
        Path path,
        Partitioning partitioning,
        Shape shape
    ) {
        return new PartitionedPathProcessors(path, partitioning, shape);
    }

    private PartitionedPaths() {
    }

    private static final class PartitionedPathProcessors implements PartitionedProcessors<Path> {

        private final Partitioned partitioned;

        private PartitionedPathProcessors(Path path, Partitioning partitioning, Shape shape) {
            partitioned = partitioned(path, partitioning, shape);
        }

        @Override
        public void close() {
            partitioned.close();
        }

        @Override
        public PartitionedProcessor<LineSegment, String> processTo(Path target, Charset charSet) {
            return new LinePartitionedProcessor(partitioned, target, charSet);
        }

        @Override
        public PartitionedProcessor<SeparatedLine, Stream<LineSegment>> processTo(
            Path target,
            Format format
        ) {
            return new FormatPartitionedProcessor(partitioned, target, format);
        }
    }
}
