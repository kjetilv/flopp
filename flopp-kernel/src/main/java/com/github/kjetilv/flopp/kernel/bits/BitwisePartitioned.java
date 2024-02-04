package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.files.FileChannelTransfers;
import com.github.kjetilv.flopp.kernel.files.FileTempTargets;
import com.github.kjetilv.flopp.kernel.files.MemoryMappedByteArrayLinesWriter;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

public class BitwisePartitioned implements Partitioned<Path> {

    private final Path path;

    private final Shape shape;

    private final List<Partition> partitions;

    public BitwisePartitioned(Path path) {
        this(path, null, null);
    }

    public BitwisePartitioned(Path path, Partitioning partitioning) {
        this(path, partitioning, null);
    }

    public BitwisePartitioned(Path path, Partitioning partitioning, Shape shape) {
        this.path = Objects.requireNonNull(path, "path");
        this.shape = shape == null ? Shape.of(path) : shape;
        this.partitions = (partitioning == null ? Partitioning.create() : partitioning)
            .of(this.shape.size());
    }

    @Override
    public Path partitioned() {
        return path;
    }

    @Override
    public List<Partition> partitions() {
        return partitions;
    }

    @Override
    public PartitionedProcessor<LineSegment> processor(Path target) {
        return new BitwisePartitionProcessor(
            mapper(),
            partitions,
            shape.charset(),
            BitwisePartitioned::writer,
            tempTargets(path),
            transfers(target)
        );
    }

    @Override
    public PartitionedMapper mapper() {
        return new BitwisePartitionedMapper(streams());
    }

    @Override
    public PartitionedConsumer consumer() {
        return new BitwisePartitionedConsumer(streams());
    }

    @Override
    public PartitionedStreams streams() {
        return new BitwisePartitionStreams(path, shape, partitions);
    }

    private static final int BUFFER_SIZE = 8192;

    private static MemoryMappedByteArrayLinesWriter writer(Path target, Charset charset) {
        return new MemoryMappedByteArrayLinesWriter(target, BUFFER_SIZE, charset);
    }

    private static Transfers<Path> transfers(Path target) {
        return new FileChannelTransfers(target);
    }

    private static TempTargets<Path> tempTargets(Path source) {
        return new FileTempTargets(source);
    }
}
