package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.partitions.Partitioning;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.stream.Stream;

final class PartitionedPathProcessors implements PartitionedProcessors<Path> {

    private final Partitioned partitioned;

    PartitionedPathProcessors(Path path, Partitioning partitioning, Shape shape) {
        partitioned = PartitionedPaths.partitioned(path, partitioning, shape);
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
