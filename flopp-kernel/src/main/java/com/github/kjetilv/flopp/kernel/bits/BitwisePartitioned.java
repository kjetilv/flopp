package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.formats.CsvFormat;
import com.github.kjetilv.flopp.kernel.io.LinesWriterFactory;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;

import java.io.Closeable;
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
    public PartitionedProcessor<LineSegment, String> processor(Path target) {
        LinesWriterFactory<Path, String> factory = path ->
            new MemoryMappedByteArrayLinesWriter(path, BUFFER_SIZE, shape.charset());
        TempTargets<Path> tempTargets = new TempDirTargets(path.getFileName().toString());
        Transfers<Path> transfers = new FileChannelTransfers(target);
        return new BitwisePartitionProcessor<>(mapper(), partitions, factory, tempTargets, transfers);
    }

    @Override
    public PartitionedProcessor<SeparatedLine, LineSegment> processor(Path target, CsvFormat format) {
        LinesWriterFactory<Path, LineSegment> linesWriterFactory = path ->
            new MemorySegmentLinesWriter(path, MEMORY_SEGMENT_SIZE);
        TempTargets<Path> tempTargets = new TempDirTargets(path.getFileName().toString());
        Transfers<Path> transfers = new FileChannelTransfers(target);

        return null;
//        new BitwisePartitionProcessor<>(
//            mapper(),
//            partitions,
//            linesWriterFactory,
//            tempTargets,
//            transfers
//        );

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

    private static final int MEMORY_SEGMENT_SIZE = 8 * BUFFER_SIZE;

    private static Partitioning partitioning(Partitioning partitioning, Shape shape) {
        return withTail(
            partitioning == null ? Partitioning.create() : partitioning,
            shape);
    }

    private static Partitioning withTail(Partitioning partitioning, Shape shape) {
        if (partitioning.tail() == 0 && shape.limitsLineLength()) {
            return partitioning.tail(shape.longestLine());
        }
        return partitioning;
    }

    @FunctionalInterface
    public interface Action extends Closeable, Consumer<LineSegment> {

        default void close() {
        }
    }
}
