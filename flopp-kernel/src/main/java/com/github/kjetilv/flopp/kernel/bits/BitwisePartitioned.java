package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.formats.CsvFormat;
import com.github.kjetilv.flopp.kernel.io.LinesWriter;
import com.github.kjetilv.flopp.kernel.io.LinesWriterFactory;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Stream;

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

        return (processor, executorService) -> {
            try (
                TempTargets<Path> tempTargets = new TempDirTargets(path);
                Transfers<Path> transfers = new FileChannelTransfers(target)
            ) {
                ResultCollector<Path> collector =
                    new ResultCollector<>(partitions.size(), path -> Shape.of(path).size());

                PartitionedMapper<LineSegment> mapper = mapper();
                CompletableFuture<Void> future = runVoid(
                    () -> mapper.map(
                            (partition, lines) -> {
                                Path tempTarget = tempTargets.temp(partition);
                                try (LinesWriter<String> writer = factory.create(tempTarget)) {
                                    lines.forEach(lineSegment ->
                                        writer.accept(processor.apply(lineSegment)));
                                }
                                return tempTarget;
                            },
                            executorService
                        )
                        .forEach(collector::collect),
                    executorService
                );
                collector.collect(transfers, executorService, future::join);
            }
        };
    }

    @Override
    public PartitionedProcessor<SeparatedLine, LineSegment> processor(Path target, CsvFormat format) {

        return (processor, executorService) -> {
            LinesWriterFactory<Path, LineSegment> linesWriterFactory = path ->
                new MemorySegmentLinesWriter(path, MEMORY_SEGMENT_SIZE);
            try (
                TempTargets<Path> tempTargets = new TempDirTargets(path);
                Transfers<Path> transfers = new FileChannelTransfers(target)
            ) {
                ResultCollector<Path> collector =
                    new ResultCollector<>(partitions.size(), path -> Shape.of(path).size());

                Stream<BitwiseCsvSplitter> splitters = streams().streamers()
                    .map(streamer ->
                        new BitwiseCsvSplitter(streamer, format));
                CompletableFuture<Void> future = runVoid(
                    () ->
                        splitters.forEach(splitter ->
                            runVoid(
                                () -> {
                                    Path temp = tempTargets.temp(splitter.partition());
                                    try (LinesWriter<LineSegment> writer = linesWriterFactory.create(temp)) {
                                        splitter.forEach(separatedLine -> {
                                            writer.accept(processor.apply(separatedLine));
                                        });
                                    }
                                },
                                executorService
                            )),
                    executorService
                );
                collector.collect(transfers, executorService, future::join);
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

    private static CompletableFuture<Void> runVoid(Runnable runnable, ExecutorService executorService) {
        return CompletableFuture.runAsync(runnable, executorService);
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
