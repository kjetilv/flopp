package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;

import java.io.Closeable;
import java.lang.foreign.MemorySegment;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

final class BitwisePartitioned implements Partitioned<Path> {

    private final Path path;

    private final Shape shape;

    private final List<Partition> partitions;

    private final MemorySegmentSource memorySegmentSource;

    BitwisePartitioned(Path path, Partitioning partitioning, Shape shape) {
        this.path = Objects.requireNonNull(path, "path");
        this.shape = shape == null ? Shape.of(path) : shape;
        this.partitions = partitioning(partitioning, this.shape).of(this.shape.size());
        this.memorySegmentSource = new MemorySegmentSource(this.path, this.shape);
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
    public PartitionedStreams streams() {
        return new BitwisePartitionStreams(path, shape, partitions, memorySegmentSource);
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
    public void close() {
        try {
            memorySegmentSource.close();
        } catch (Exception e) {
            throw new RuntimeException(STR."\{this} could not close", e);
        }
    }

    private static final int BUFFER_SIZE = 8192;

    private static Partitioning partitioning(Partitioning partitioning, Shape shape) {
        return withTail(
            partitioning == null ? Partitioning.create() : partitioning,
            shape
        );
    }

    private static Partitioning withTail(Partitioning partitioning, Shape shape) {
        return partitioning.tail() == 0 && shape.limitsLineLength()
            ? partitioning.tail(shape.longestLine())
            : partitioning;
    }

    private static MemoryMappedByteArrayLinesWriter writer(Path target, Charset charset) {
        return new MemoryMappedByteArrayLinesWriter(target, BUFFER_SIZE, charset);
    }

    private static Transfers<Path> transfers(Path target) {
        return new FileChannelTransfers(target);
    }

    private static TempTargets<Path> tempTargets(Path source) {
        return new FileTempTargets(source);
    }

    @FunctionalInterface
    public interface Action extends Closeable {

        void line(MemorySegment segment, long startIndex, long endIndex);

        @Override
        default void close() {
        }
    }
}
