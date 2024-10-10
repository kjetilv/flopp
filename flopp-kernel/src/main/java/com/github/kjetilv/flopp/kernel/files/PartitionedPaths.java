package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Partitioned;
import com.github.kjetilv.flopp.kernel.PartitionedProcessor;
import com.github.kjetilv.flopp.kernel.Partitioning;
import com.github.kjetilv.flopp.kernel.Shape;
import com.github.kjetilv.flopp.kernel.formats.Format;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public final class PartitionedPaths {

    public static Partitioned<Path> partitioned(Path path) {
        return partitioned(path, null, null);
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

    public static PartitionedProcessor<Path, LineSegment, String> processor(
        Path path,
        Partitioning partitioning,
        Shape shape,
        Charset charset
    ) {
        return processor(partitioned(path, partitioning, shape), charset);
    }

    public static PartitionedProcessor<Path, SeparatedLine, Stream<LineSegment>> processor(
        Path path,
        Partitioning partitioning,
        Shape shape,
        Format format
    ) {
        return processor(partitioned(path, partitioning, shape), format);
    }

    public static PartitionedProcessor<Path, LineSegment, String> processor(
        Partitioned<Path> partitioned, Charset charset
    ) {
        return new LinePartitionedProcessor(partitioned, charset);
    }

    public static PartitionedProcessor<Path, SeparatedLine, Stream<LineSegment>> processor(
        Partitioned<Path> partitioned,
        Format format
    ) {
        return new FormatPartitionedProcessor(partitioned, format);
    }

    private PartitionedPaths() {
    }
}
