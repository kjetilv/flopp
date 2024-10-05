package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.formats.FlatFileFormat;
import com.github.kjetilv.flopp.kernel.io.LinesWriter;
import com.github.kjetilv.flopp.kernel.io.LinesWriterFactory;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.StructuredTaskScope;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

@SuppressWarnings("preview")
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
        return (processor, executor) -> {
            LinesWriterFactory<Path, String> writers = path ->
                new MemoryMappedByteArrayLinesWriter(path, BUFFER_SIZE, shape.charset());
            ResultCollector<Path> collector =
                new ResultCollector<>(partitions.size(), sizer(), executor);
            try (
                TempTargets<Path> tempTargets = new TempDirTargets(path);
                Transfers<Path> transfers = new FileChannelTransfers(target)
            ) {
                try (StructuredTaskScope<PartitionResult<Path>> scope = new StructuredTaskScope<>()) {
                    streams().streamers()
                        .forEach(streamer ->
                            scope.fork(() -> {
                                Path tempTarget = tempTargets.temp(streamer.partition());
                                try (LinesWriter<String> writer = writers.create(tempTarget)) {
                                    streamer.lines()
                                        .map(processor.apply(streamer.partition()))
                                        .forEach(writer);
                                }
                                collector.sync(new PartitionResult<>(streamer.partition(), tempTarget));
                                return null;
                            }));
                    try {
                        scope.join();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted", e);
                    }
                }
                collector.syncTo(transfers);
            }
        };
    }

    @Override
    public PartitionedProcessor<SeparatedLine, Stream<LineSegment>> processor(Path target, FlatFileFormat format) {
        return (processor, executor) -> {
            LinesWriterFactory<Path, Stream<LineSegment>> writers = path ->
                new LineSegmentsWriter(path, MEMORY_SEGMENT_SIZE);
            ResultCollector<Path> collector = new ResultCollector<>(partitions.size(), sizer(), executor);
            try (
                TempTargets<Path> tempTargets = new TempDirTargets(path);
                Transfers<Path> transfers = new FileChannelTransfers(target)
            ) {
                try (StructuredTaskScope<PartitionResult<Path>> scope = new StructuredTaskScope<>()) {
                    splitters().splitters(format)
                        .forEach(splitter ->
                            scope.fork(() -> {
                                Path tempTarget = tempTargets.temp(splitter.partition());
                                try (LinesWriter<Stream<LineSegment>> writer = writers.create(tempTarget)) {
                                    splitter.separatedLines()
                                        .map(processor.apply(splitter.partition()))
                                        .forEach(writer);
                                }
                                collector.sync(new PartitionResult<>(splitter.partition(), tempTarget));
                                return null;
                            }));
                    try {
                        scope.join();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted", e);
                    }
                }
                collector.syncTo(transfers);
            }
        };
    }

    @Override
    public PartitionedMapper<LineSegment> mapper() {
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

    private static ToLongFunction<Path> sizer() {
        return path -> Shape.of(path).size();
    }

    private static Partitioning partitioning(Partitioning partitioning, Shape shape) {
        return withTail(
            partitioning == null ? Partitioning.create() : partitioning,
            shape
        );
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
