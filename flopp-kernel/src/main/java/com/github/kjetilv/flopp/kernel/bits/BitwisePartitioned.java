package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.formats.Partitioning;
import com.github.kjetilv.flopp.kernel.formats.Shape;
import com.github.kjetilv.flopp.kernel.io.LinesWriter;
import com.github.kjetilv.flopp.kernel.io.TempTargets;
import com.github.kjetilv.flopp.kernel.io.Transfers;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Consumer;

final class BitwisePartitioned implements Partitioned<Path> {

    private final Path path;

    private final Shape shape;

    private final Partitions partitions;

    private final MemorySegmentSource memorySegmentSource;

    BitwisePartitioned(Path path, Partitioning partitioning, Shape shape) {
        this.path = Objects.requireNonNull(path, "path");
        this.shape = shape == null ? Shape.of(path) : shape;
        this.partitions = partitioning(partitioning, this.shape).of(this.shape.size());
        this.memorySegmentSource = new PartialMemorySegmentSource(this.path, this.shape);
    }

    @Override
    public Path partitioned() {
        return path;
    }

    @Override
    public Partitions partitions() {
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
    public PartitionedSplitters splitters() {
        return new BitwisePartitionedSplitters(streams(), shape);
    }

    @Override
    public PartitionedStreams streams() {
        return new BitwisePartitionStreams(shape, partitions, memorySegmentSource);
    }

    @Override
    public void close() {
        try {
            memorySegmentSource.close();
        } catch (Exception e) {
            throw new RuntimeException(this + " could not close", e);
        }
    }

    private static final int BUFFER_SIZE = 8192;

    private static LinesWriter writer(Path target, Charset charset) {
        return new MemoryMappedByteArrayLinesWriter(target, BUFFER_SIZE, charset);
    }

    private static TempTargets<Path> tempTargets(Path path) {
        return new PathTempTargets(path.getFileName().toString());
    }

    private static Partitioning partitioning(Partitioning partitioning, Shape shape) {
        return withTail(partitioning == null ? Partitioning.create() : partitioning, shape);
    }

    private static Partitioning withTail(Partitioning partitioning, Shape shape) {
        return partitioning.tail() == 0 && shape.limitsLineLength()
            ? partitioning.tail(shape.longestLine())
            : partitioning;
    }

    private static Transfers<Path> transfers(Path target) {
        return new FileChannelTransfers(target);
    }

    @FunctionalInterface
    public interface Action extends Closeable, Consumer<LineSegment> {

        default void close() {
        }
    }
}
